import abc
import logging
import os
from abc import ABC
from typing import Dict, List

from common.linker.linker import Linker
from common.packets.aggregator_packets import ChunkOrStop, StopPacket
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket
from common.rabbit_middleware import Rabbit

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
CHUNK_SIZE = 256
SEND_DELAY_SEC = 0.1


class BasicAggregator(ABC):
    def __init__(self, replica_id: int, side_table_queue: str):

        self._rabbit = Rabbit(RABBIT_HOST)
        input_queue = Linker().get_input_queue(self, replica_id)
        self._rabbit.consume(input_queue, self.__on_stream_message_callback)
        eof_routing_key = Linker().get_eof_out_routing_key(self)
        logging.info(f"Routing packets to {input_queue} using routing key {eof_routing_key}")
        self._rabbit.route(input_queue, "control", eof_routing_key)

        self._rabbit.subscribe(side_table_queue, self.__on_side_table_message_callback)

    def __on_side_table_message_callback(self, msg: bytes) -> bool:
        decoded = ChunkOrStop.decode(msg)
        if isinstance(decoded.data, list):
            for message in decoded.data:
                self.handle_side_table_message(message)
        elif isinstance(decoded.data, StopPacket):
            logging.info(f"action: side_table_receive_message | result: stop | city_name: {decoded.data.city_name}")
            outgoing_messages = self.handle_stop(decoded.data.city_name)
            self.__send_messages(outgoing_messages)
        else:
            raise ValueError(f"Unexpected message type: {type(decoded.data)}")

        return True

    def __handle_chunk(self, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def __send_messages(self, outgoing_messages: Dict[str, List[bytes]]):
        for (queue, messages) in outgoing_messages.items():
            if queue.endswith("_eof_in"):
                for message in messages:
                    self._rabbit.produce(queue, message)
            if len(messages) > 1:
                encoded = GenericPacket(messages).encode()
                self._rabbit.produce(queue, encoded)

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)
        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof(decoded.data)
        elif isinstance(decoded.data, bytes):
            outgoing_messages = self.handle_message(decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(decoded.data)
        else:
            raise Exception(f"Unknown message type: {type(decoded.data)}")

        self.__send_messages(outgoing_messages)

        return True

    @abc.abstractmethod
    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_side_table_message(self, message: bytes):
        pass

    @abc.abstractmethod
    def handle_stop(self, city_name: str) -> Dict[str, List[bytes]]:
        pass

    def start(self):
        self._rabbit.start()
