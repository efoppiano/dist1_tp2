import abc
import logging
import os
import pickle
from abc import ABC
from typing import List, Dict, Union

from common.components.heartbeater.heartbeater import HeartBeater
from common.components.message_sender import MessageSender
from common.components.state_saver import Recoverable, StateSaver
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket, GenericPacketBuilder
from common.middleware.rabbit_middleware import Rabbit

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]


class BasicFilter(Recoverable, ABC):
    def __init__(self, container_id: str):
        self._starting_up = True
        logging.info(
            f"action: init | result: in_progress | filter: {self.__class__.__name__} | container_id: {container_id}")
        self.__setup_middleware()

        self.basic_filter_container_id = container_id
        self._message_sender = MessageSender(self._rabbit)
        self.heartbeater = HeartBeater()
        self.state_saver = StateSaver(self)
        self._starting_up = False

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
            encoded = msg
        else:
            decoded = msg
            encoded = msg.encode()

        flow_id = decoded.get_flow_id()

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof_message(flow_id, decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise ValueError(f"Unknown packet type: {type(decoded.data)}")

        builder = GenericPacketBuilder(self.basic_filter_container_id, decoded.client_id, decoded.city_name)
        self._message_sender.send(builder, outgoing_messages, skip_send=self._starting_up)

        if not self._starting_up:
            self.state_saver.save_state(encoded)

        return True

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_eof_message(self, flow_id, message: Eof) -> Dict[str, Union[List[bytes], Eof]]:
        pass

    def set_state(self, state_bytes: bytes):
        state = pickle.loads(state_bytes)
        self._message_sender.set_state(state["message_sender"])

    def get_state(self) -> bytes:
        state = {
            "message_sender": self._message_sender.get_state()
        }
        return pickle.dumps(state)

    def replay(self, msg: bytes) -> None:
        self.on_message_callback(msg)

    def start(self):
        self.heartbeater.start()
        self._rabbit.start()
