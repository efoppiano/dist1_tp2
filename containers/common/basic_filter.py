import abc
import os
from abc import ABC
from typing import List, Dict, Union

from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket
from common.rabbit_middleware import Rabbit

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")


class BasicFilter(ABC):
    def __init__(self, replica_id: int):
        self._input_queue = Linker().get_input_queue(self, replica_id)
        self._rabbit = Rabbit(RABBIT_HOST)
        self._rabbit.consume(self._input_queue, self.on_message_callback)
        eof_routing_key = Linker().get_eof_out_routing_key(self)
        self._rabbit.route(self._input_queue, "control", eof_routing_key)

        self._basic_filter_replica_id = replica_id

    def __handle_chunk(self, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def on_message_callback(self, msg: Union[bytes, GenericPacket]) -> bool:
        if isinstance(msg, bytes):
            decoded = GenericPacket.decode(msg)
        else:
            decoded = msg
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
                for message in messages:
                    self._rabbit.produce(queue, message)
            if len(messages) > 1:
                encoded = GenericPacket(self._basic_filter_replica_id, messages).encode()
                self._rabbit.produce(queue, encoded)

        return True

    @abc.abstractmethod
    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        pass

    def start(self):
        self._rabbit.start()
