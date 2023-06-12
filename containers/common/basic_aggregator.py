import abc
import logging
import os
import pickle
from abc import ABC
from typing import Dict, List, Union

from common.router import MultiRouter
from common.utils import save_state, load_state, min_hash
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket, PacketIdentifier
from common.rabbit_middleware import Rabbit

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
PREV_AMOUNT = int(os.environ["PREV_AMOUNT"])
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
MAX_PACKET_ID = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability


class BasicAggregator(ABC):
    def __init__(self, router: MultiRouter, replica_id: int, side_table_routing_key: str):
        self._basic_agg_replica_id = replica_id

        self._rabbit = Rabbit(RABBIT_HOST)
        input_queue = INPUT_QUEUE
        self._rabbit.consume(input_queue, self.__on_stream_message_callback)
        eof_routing_key = EOF_ROUTING_KEY
        logging.info(f"Routing packets to {input_queue} using routing key {eof_routing_key}")
        self._rabbit.route(input_queue, "publish", eof_routing_key)

        self._rabbit.route(input_queue, "publish", side_table_routing_key)

        self._last_received = {}
        self._last_packet_id = 0
        self._eofs_received = {}

        self.router = router

        state = self.__load_full_state()
        if state is not None:
            self.__set_full_state(state)

    def __handle_chunk(self, flow_id, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(flow_id, message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def __send_messages(self, id: PacketIdentifier, outgoing_messages: Dict[str, Union[List[bytes], Eof]]):
        for (queue, messages_or_eof) in outgoing_messages.items():
            if isinstance(messages_or_eof, Eof) or len(messages_or_eof) > 0:
                encoded = GenericPacket(
                    replica_id=id.replica_id,
                    client_id=id.client_id,
                    city_name=id.city_name,
                    packet_id=self.__get_next_packet_id(),
                    data=messages_or_eof
                ).encode()
                if queue.startswith("publish_"):
                    queue = queue[len("publish_"):]
                    logging.debug(
                        f"Sending {id.replica_id}-{id.packet_id}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.send_to_route("publish", queue, encoded)
                else:
                    logging.debug(
                        f"Sending {id.replica_id}-{id.packet_id}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.produce(queue, encoded)

    def __on_stream_message_without_duplicates(self, id: PacketIdentifier, decoded: GenericPacket) -> bool:
        flow_id = (decoded.client_id, decoded.city_name)

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof_message(flow_id, decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise Exception(f"Unknown message type: {type(decoded.data)}")

        self.__send_messages(id, outgoing_messages)

        return True

    def __update_last_received(self, packet: GenericPacket):
        replica_id = packet.replica_id

        current_id = packet.packet_id
        last_id = self._last_received.get(replica_id)
        if current_id == last_id:
            logging.warning(f"Received duplicate {replica_id}-{current_id}-{min_hash(packet.data)} - ignoring")
            return False
        logging.debug(f"Received {replica_id}-{current_id}-{min_hash(packet.data)}")
        self._last_received[replica_id] = current_id

        return True

    def __get_next_packet_id(self) -> int:
        self._last_packet_id = (self._last_packet_id + 1) % MAX_PACKET_ID
        return self._last_packet_id

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)

        if not self.__update_last_received(decoded):
            return True

        id = PacketIdentifier(
            self._basic_agg_replica_id,
            decoded.client_id,
            decoded.city_name,
            self.__get_next_packet_id()
        )
        if not self.__on_stream_message_without_duplicates(id, decoded):
            return False

        self.__save_full_state()
        return True

    def handle_eof_message(self, flow_id, message: Eof) -> Dict[str, Eof]:
        self._eofs_received.setdefault(flow_id, 0)
        self._eofs_received[flow_id] += 1

        if self._eofs_received[flow_id] < PREV_AMOUNT:
            return {}

        self._eofs_received.pop(flow_id)

        return self.handle_eof(flow_id, message)

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    def handle_eof(self, flow_id, message: Eof) -> Dict[str, Eof]:
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
            "_last_received": self._last_received,
            "_last_packet_id": self._last_packet_id,
            "_eofs_received": self._eofs_received,
        }
        save_state(pickle.dumps(state))

    def __load_full_state(self) -> Union[dict, None]:
        state = load_state()
        if not state:
            return None
        return pickle.loads(state)

    def __set_full_state(self, state: dict):
        self.set_state(state["concrete_state"])
        self._last_received = state["_last_received"]
        self._last_packet_id = state["_last_packet_id"]
        self._eofs_received = state["_eofs_received"]
