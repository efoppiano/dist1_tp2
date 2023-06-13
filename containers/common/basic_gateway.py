import abc
import logging
import os
import pickle
from abc import ABC
from typing import Dict, List, Union

from common.packets.client_packet import ClientPacket
from common.router import MultiRouter
from common.utils import save_state, load_state, min_hash
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacketBuilder
from common.rabbit_middleware import Rabbit

CLIENT_QUEUE = os.environ["CLIENT_QUEUE"]
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
MAX_SEQ_NUMBER = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability


class BasicGateway(ABC):
    def __init__(self, router: MultiRouter, replica_id: int):
        self.__setup_middleware()

        self._basic_gateway_replica_id = replica_id
        self._last_chunk_received = None
        self._last_eof_received = None
        self._last_seq_number = 0
        self.router = router

        self.__setup_state()

    def __setup_state(self):
        state = self.__load_full_state()
        if state is not None:
            self.__set_full_state(state)

    def __setup_middleware(self):
        self._rabbit = Rabbit(RABBIT_HOST)
        input_queue = CLIENT_QUEUE
        self._rabbit.consume(input_queue, self.__on_stream_message_callback)
        eof_routing_key = EOF_ROUTING_KEY
        logging.info(f"Routing packets to {input_queue} using routing key {eof_routing_key}")
        self._rabbit.route(input_queue, "publish", eof_routing_key)

    def __handle_chunk(self, flow_id, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(flow_id, message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def __send_messages(self, builder: GenericPacketBuilder, outgoing_messages: Dict[str, Union[List[bytes], Eof]]):
        for (queue, messages_or_eof) in outgoing_messages.items():
            if isinstance(messages_or_eof, Eof) or len(messages_or_eof) > 0:
                encoded = builder.build(self.__get_next_seq_number(), messages_or_eof).encode()
                if queue.startswith("publish_"):
                    queue = queue[len("publish_"):]
                    logging.debug(
                        f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.send_to_route("publish", queue, encoded)
                else:
                    logging.debug(
                        f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.produce(queue, encoded)

    def __on_stream_message_without_duplicates(self, builder: GenericPacketBuilder, decoded: ClientPacket) -> bool:
        flow_id = decoded.get_flow_id()

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof(decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise Exception(f"Unknown message type: {type(decoded.data)}")

        self.__send_messages(builder, outgoing_messages)

        return True

    def __update_last_received(self, packet: ClientPacket) -> bool:
        packet_id = packet.get_id()

        if packet.is_eof():
            if packet_id == self._last_eof_received:
                logging.warning(
                    f"Received duplicate EOF {packet_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_eof_received = packet_id
        elif packet.is_chunk():
            if packet_id == self._last_chunk_received:
                logging.warning(
                    f"Received duplicate chunk {packet_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_chunk_received = packet_id

        logging.debug(f"Received {packet_id}-{min_hash(packet.data)}")

        return True

    def __get_next_seq_number(self) -> int:
        self._last_seq_number = (self._last_seq_number + 1) % MAX_SEQ_NUMBER
        return self._last_seq_number

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = ClientPacket.decode(msg)

        if not self.__update_last_received(decoded):
            return True

        builder = GenericPacketBuilder(self._basic_gateway_replica_id, decoded.client_id, decoded.city_name)
        if not self.__on_stream_message_without_duplicates(builder, decoded):
            return False

        self.__save_full_state()
        return True

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    def handle_eof(self, message: Eof) -> Dict[str, Eof]:
        eof_output_queue = self.router.publish()
        output = {}
        for queue in eof_output_queue:
            output[queue] = message

        return output

    def start(self):
        self._rabbit.start()

    @abc.abstractmethod
    def get_state(self) -> bytes:
        pass

    @abc.abstractmethod
    def set_state(self, state: bytes):
        pass

    def __save_full_state(self):
        state = {
            "concrete_state": self.get_state(),
            "_last_eof_received": self._last_eof_received,
            "_last_chunk_received": self._last_chunk_received,
            "_last_seq_number": self._last_seq_number,
        }
        save_state(pickle.dumps(state))

    @staticmethod
    def __load_full_state() -> Union[dict, None]:
        state = load_state()
        if not state:
            return None
        return pickle.loads(state)

    def __set_full_state(self, state: dict):
        self.set_state(state["concrete_state"])
        self._last_eof_received = state["_last_eof_received"]
        self._last_chunk_received = state["_last_chunk_received"]
        self._last_seq_number = state["_last_seq_number"]
