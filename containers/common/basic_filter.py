import abc
import os
from abc import ABC
from typing import List, Dict

from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket
from common.rabbit_middleware import Rabbit
from common.utils import build_eof_out_queue_name, build_queue_name

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
CHUNK_SIZE = 256
SEND_DELAY_SEC = 0.1


class BasicFilter(ABC):
    def __init__(self, input_queue_name: str, replica_id: int):
        self._input_queue = build_queue_name(input_queue_name, replica_id)
        self._rabbit = Rabbit(RABBIT_HOST)
        self._rabbit.consume(self._input_queue, self.__on_message_callback)
        self._rabbit.route(self._input_queue, "control", build_eof_out_queue_name(input_queue_name))

        self.__eof_messages_buffer = {}
        self.__basic_filter_buffer = {}
        self._call_later_set = False

    def __send_messages(self, queue: str, messages: List[bytes]):
        chunks = [messages[i:i + CHUNK_SIZE] for i in range(0, len(messages), CHUNK_SIZE)]
        for chunk in chunks:
            message = GenericPacket(chunk)
            self._rabbit.produce(queue, message.encode())

    def __send_chunks(self):
        for (queue, messages) in self.__basic_filter_buffer.items():
            self.__send_messages(queue, messages)

        for (queue, messages) in self.__eof_messages_buffer.items():
            for message in messages:
                self._rabbit.produce(queue, message)

        self.__basic_filter_buffer = {}
        self._call_later_set = False

    def __handle_chunk(self, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def __on_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)
        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof(decoded.data)
        elif isinstance(decoded.data, bytes):
            outgoing_messages = self.handle_message(decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(decoded.data)
        else:
            raise ValueError(f"Unknown packet type: {type(decoded.data)}")

        for (queue, messages) in outgoing_messages.items():
            if queue.endswith("_eof_in"):
                self.__eof_messages_buffer.setdefault(queue, [])
                self.__eof_messages_buffer[queue] += messages
            else:
                self.__basic_filter_buffer.setdefault(queue, [])
                self.__basic_filter_buffer[queue] += messages

        if not self._call_later_set and outgoing_messages != {}:
            self._call_later_set = True
            self._rabbit.call_later(SEND_DELAY_SEC, self.__send_chunks)

        return True

    @abc.abstractmethod
    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        pass

    def start(self):
        self._rabbit.start()
