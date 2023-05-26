import abc
import logging
import os
from abc import ABC
from typing import List, Dict

from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.generic_packet import GenericPacket
from common.rabbit_middleware import Rabbit

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
CHUNK_SIZE = 512


class BasicSynchronizer(ABC):
    def __init__(self, input_queues: List[str]):
        self._input_queues = input_queues
        self._rabbit = Rabbit(RABBIT_HOST)
        for input_queue in self._input_queues:
            self._rabbit.consume(input_queue,
                                 lambda msg, input_queue=input_queue: self.__on_message_callback(input_queue, msg))

    def __on_message_callback(self, queue: str, msg: bytes) -> bool:
        try:
            decoded = EofWithId.decode(msg)
        except Exception:
            decoded = Eof.decode(msg)
            raise ValueError(f"Unknown packet: {decoded}")

        outgoing_messages = self.handle_message(queue, decoded)

        for (routing_key, messages) in outgoing_messages.items():
            for message in messages:
                self._rabbit.send_to_route("control", routing_key, message)
        return True

    @abc.abstractmethod
    def handle_message(self, queue: str, message: EofWithId) -> Dict[str, List[bytes]]:
        pass

    def start(self):
        self._rabbit.start()
