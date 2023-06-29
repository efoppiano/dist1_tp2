import logging
import typing
from typing import Dict, Union, List

from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacketBuilder
from common.middleware.rabbit_middleware import Rabbit
from common.utils import min_hash, log_msg

MAX_SEQ_NUMBER = 2 ** 9

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
        queue_prefix = queue[:-2]
        if queue not in self._last_seq_number and queue_prefix in self._last_seq_number:
            self._last_seq_number[queue] = self._last_seq_number[queue_prefix]
        else:
            self._last_seq_number.setdefault(queue, 0)
        self._last_seq_number[queue] += 1

        if self._last_seq_number[queue] > MAX_SEQ_NUMBER:
            self._last_seq_number[queue] = 0
            self._times_maxed_seq += 1
            log_msg("Generated %d packets [%d]", MAX_SEQ_NUMBER, self._times_maxed_seq)

        return self._last_seq_number[queue]

    def __get_next_publish_seq_number(self, queue: str) -> int:
        self._last_seq_number.setdefault(queue, -1)
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
                    encoded = builder.build(self.__get_next_publish_seq_number(queue), messages_or_eof).encode()
                    if not skip_send:
                        logging.debug(
                            f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                        self._rabbit.send_to_route("publish", queue, encoded)
                else:
                    encoded = builder.build(self.__get_next_seq_number(queue), messages_or_eof).encode()
                    if not skip_send:
                        logging.debug(
                            f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                        self._rabbit.produce(queue, encoded)

    def get_state(self) -> dict:
        return {
            "last_seq_number": self._last_seq_number
        }

    def set_state(self, state: dict):
        self._last_seq_number = state["last_seq_number"]
