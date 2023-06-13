import abc
import logging
import os
import pickle
from abc import ABC
from typing import List, Dict, Union, Optional

from common.message_sender import MessageSender
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket, GenericPacketBuilder
from common.rabbit_middleware import Rabbit
from common.utils import load_state, save_state

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]


class BasicFilter(ABC):
    def __init__(self, replica_id: int):
        logging.info(
            f"action: init | result: in_progress | filter: {self.__class__.__name__} | replica_id: {replica_id}")
        self.__setup_middleware()

        self.basic_filter_replica_id = replica_id
        self._message_sender = MessageSender(self._rabbit)

        state = self.__load_full_state()
        if state is not None:
            logging.info(f"Found previous state, setting it")
            self.set_state(state)

    def __setup_middleware(self):
        self._input_queue = INPUT_QUEUE
        self._rabbit = Rabbit(RABBIT_HOST)
        self._rabbit.consume(self._input_queue, self.on_message_callback)
        eof_routing_key = EOF_ROUTING_KEY
        self._rabbit.route(self._input_queue, "publish", eof_routing_key)

    def __handle_chunk(self, flow_id, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(flow_id, message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def on_message_callback(self, msg: Union[bytes, GenericPacket]) -> bool:
        if isinstance(msg, bytes):
            decoded = GenericPacket.decode(msg)
        else:
            decoded = msg

        builder = GenericPacketBuilder(self.basic_filter_replica_id, decoded.client_id, decoded.city_name)

        self.__on_message_without_duplicates(builder, decoded)

        return True

    def __on_message_without_duplicates(self, builder: GenericPacketBuilder, decoded: GenericPacket) -> bool:
        flow_id = decoded.get_flow_id()

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof_message(flow_id, decoded.data)
        elif isinstance(decoded.data, bytes):
            outgoing_messages = self.handle_message(flow_id, decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise ValueError(f"Unknown packet type: {type(decoded.data)}")

        self._message_sender.send(builder, outgoing_messages)

        return True

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_eof_message(self, flow_id, message: Eof) -> Dict[str, Union[List[bytes], Eof]]:
        pass

    @abc.abstractmethod
    def get_next_seq_number(self) -> int:
        pass

    @staticmethod
    def __load_full_state() -> Optional[dict]:
        state = load_state()
        if not state:
            return None
        return pickle.loads(state)

    def __save_full_state(self):
        state = self.get_state()
        save_state(pickle.dumps(state))

    def set_state(self, state: dict):
        self._message_sender = state["message_sender"]

    def get_state(self) -> dict:
        return {
            "message_sender": self._message_sender,
        }

    def start(self):
        self._rabbit.start()
