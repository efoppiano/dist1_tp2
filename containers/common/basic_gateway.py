import abc
import logging
import os
import pickle
import time
from abc import ABC
from typing import Dict, List

from common.heartbeater import HeartBeater
from common.message_sender import MessageSender
from common.packets.client_packet import ClientDataPacket, ClientPacket
from common.readers import ClientIdResponsePacket
from common.router import Router
from common.utils import save_state, load_state, min_hash
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacketBuilder
from common.rabbit_middleware import Rabbit

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]

NEXT = os.environ["NEXT"]
NEXT_AMOUNT = int(os.environ["NEXT_AMOUNT"])

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
MAX_SEQ_NUMBER = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability


class BasicGateway(ABC):
    def __init__(self, replica_id: int):
        self.__setup_middleware()

        self._basic_gateway_replica_id = replica_id
        self._last_chunk_received = None
        self._last_eof_received = None
        self._message_sender = MessageSender(self._rabbit)
        self.router = Router(NEXT, NEXT_AMOUNT)
        self.heartbeater = HeartBeater(self._rabbit)

        self.__setup_state()

    def __setup_state(self):
        state = load_state()
        if state is not None:
            self.set_state(state)

    def __setup_middleware(self):
        self._rabbit = Rabbit(RABBIT_HOST)
        input_queue = INPUT_QUEUE
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

    def __on_stream_message_without_duplicates(self, decoded: ClientDataPacket) -> bool:
        flow_id = decoded.get_flow_id()

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof(decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise Exception(f"Unknown message type: {type(decoded.data)}")

        builder = GenericPacketBuilder(self._basic_gateway_replica_id, decoded.client_id, decoded.city_name)
        self._message_sender.send(builder, outgoing_messages)

        return True

    def __update_last_received(self, packet: ClientDataPacket) -> bool:
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

    def __generate_and_send_client_id(self):
        new_client_id = f"{self._basic_gateway_replica_id}_{time.time_ns()}"
        logging.info(f"New client id: {new_client_id}")

        response = ClientIdResponsePacket(new_client_id).encode()

        self._rabbit.produce("client_id_queue", response)

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = ClientPacket.decode(msg)
        if not isinstance(decoded.data, ClientDataPacket):
            self.__generate_and_send_client_id()
            return True

        if not self.__update_last_received(decoded.data):
            return True

        if not self.__on_stream_message_without_duplicates(decoded.data):
            return False

        save_state(self.get_state())
        return True

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    def handle_eof(self, message: Eof) -> Dict[str, Eof]:
        eof_output_queue = self.router.publish()

        return {
            eof_output_queue: message
        }

    def start(self):
        self.heartbeater.start()
        self._rabbit.start()

    def get_state(self) -> bytes:
        state = {
            "message_sender": self._message_sender.get_state(),
            "last_chunk_received": self._last_chunk_received,
            "last_eof_received": self._last_eof_received,
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._message_sender.set_state(state["message_sender"])
        self._last_chunk_received = state["last_chunk_received"]
        self._last_eof_received = state["last_eof_received"]
