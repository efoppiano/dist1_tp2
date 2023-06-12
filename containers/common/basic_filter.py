import abc
import logging
import os
from abc import ABC
from typing import List, Dict, Union

from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket, PacketIdentifier
from common.rabbit_middleware import Rabbit
from common.utils import min_hash

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]


class BasicFilter(ABC):
    def __init__(self, replica_id: int):
        logging.info(
            f"action: init | result: in_progress | filter: {self.__class__.__name__} | replica_id: {replica_id}")
        self._input_queue = INPUT_QUEUE
        self._rabbit = Rabbit(RABBIT_HOST)
        self._rabbit.consume(self._input_queue, self.on_message_callback)
        eof_routing_key = EOF_ROUTING_KEY
        self._rabbit.route(self._input_queue, "publish", eof_routing_key)

        self.basic_filter_replica_id = replica_id

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

        id = PacketIdentifier(
            replica_id=decoded.replica_id,
            client_id=decoded.client_id,
            city_name=decoded.city_name,
            packet_id=decoded.packet_id
        )

        self.__on_message_without_duplicates(id, decoded)

        return True

    def __on_message_without_duplicates(self, id: PacketIdentifier, decoded: GenericPacket) -> bool:
        flow_id = (id.client_id, id.city_name)

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof_message(flow_id, decoded.data)
        elif isinstance(decoded.data, bytes):
            outgoing_messages = self.handle_message(flow_id, decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise ValueError(f"Unknown packet type: {type(decoded.data)}")

        self.send_messages(id, outgoing_messages)

        return True

    def send_messages(self, id: PacketIdentifier, outgoing_messages: Dict[str, Union[List[bytes], Eof]]):

        for (queue, messages_or_eof) in outgoing_messages.items():
            if isinstance(messages_or_eof, Eof) or len(messages_or_eof) > 0:
                encoded = GenericPacket(
                    replica_id=id.replica_id,
                    client_id=id.client_id,
                    city_name=id.city_name,
                    packet_id=self.get_next_packet_id(),
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

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_eof_message(self, flow_id, message: Eof) -> Dict[str, Union[List[bytes], Eof]]:
        pass

    @abc.abstractmethod
    def get_next_packet_id(self) -> int:
        pass

    def start(self):
        self._rabbit.start()
