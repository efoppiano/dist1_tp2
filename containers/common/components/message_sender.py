import logging
import random
import typing
from typing import Dict, Union, List

from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacketBuilder
from common.middleware.rabbit_middleware import Rabbit
from common.utils import min_hash, log_msg

MAX_SEQ_NUMBER = 2

MessageData = typing.NewType("MessageData", bytes)
MessageContent = typing.NewType("MessageContent", Union[List[MessageData], Eof])
QueueOrRoutingKey = typing.NewType("QueueOrRoutingKey", str)
OutgoingMessages = typing.NewType("OutgoingMessages", Dict[QueueOrRoutingKey, MessageContent])


class MessageSender:
    def __init__(self, middleware: Rabbit):
        self._last_seq_number: Dict[str, int] = {}
        self._rabbit = middleware

        self._times_maxed_seq = 0

    def __get_next_seq_number(self, queue: str) -> int:
        self._last_seq_number.setdefault(queue, 0)
        self._last_seq_number[queue] += 1

        if self._last_seq_number[queue] > MAX_SEQ_NUMBER:
            self._last_seq_number[queue] = 1
            self._times_maxed_seq += 1
            log_msg("Generated %d packets [%d]", MAX_SEQ_NUMBER, self._times_maxed_seq)

        return self._last_seq_number[queue]

    def __get_next_publish_seq_number(self, queue: str) -> int:
        self._last_seq_number.setdefault(queue, 0)
        self._last_seq_number[queue] -= 1

        if self._last_seq_number[queue] < -MAX_SEQ_NUMBER:
            self._last_seq_number[queue] = -1
            self._times_maxed_seq += 1
            log_msg("Generated %d packets [%d]", MAX_SEQ_NUMBER, self._times_maxed_seq)

        return self._last_seq_number[queue]

    def send(self, builder: GenericPacketBuilder, outgoing_messages: OutgoingMessages,
             skip_send=False):
        for (queue, messages_or_eof) in outgoing_messages.items():
            if isinstance(messages_or_eof, Eof) or len(messages_or_eof) > 0:
                if queue.startswith("publish_"):
                    queue = queue[len("publish_"):]
                    seq_number = self.__get_next_publish_seq_number(queue)
                    packet = builder.build(seq_number, messages_or_eof)
                    if not skip_send:
                        logging.debug(
                            f"Sending eof {packet.get_id_and_hash()} to {queue}")
                        self._rabbit.send_to_route("publish", queue, packet.encode())
                else:
                    seq_number = self.__get_next_seq_number(queue)
                    packet = builder.build(seq_number, messages_or_eof)
                    if not skip_send:
                        logging.debug(
                            f"Sending chunk {packet.get_id_and_hash()} to {queue}")
                        self._rabbit.produce(queue, packet.encode())

    def get_state(self) -> dict:
        return {
            "last_seq_number": self._last_seq_number
        }

    def set_state(self, state: dict):
        self._last_seq_number = state["last_seq_number"]
