import logging
import pickle
from typing import Dict, Union, List

from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacketBuilder
from common.middleware.rabbit_middleware import Rabbit
from common.utils import min_hash, log_msg

MAX_SEQ_NUMBER = 2 ** 9


class MessageSender:
    def __init__(self, middleware: Rabbit):
        self._last_seq_number: Dict[str, int] = {}
        self._rabbit = middleware

        self._times_maxed_seq = 0

    def __get_next_seq_number(self, queue: str) -> int:
        self._last_seq_number.setdefault(queue, 0)
        self._last_seq_number[queue] += 1

        if self._last_seq_number[queue] > MAX_SEQ_NUMBER:
            self._last_seq_number[queue] = 0
            self._times_maxed_seq += 1
            log_msg("Generated %d packets [%d]", MAX_SEQ_NUMBER, self._times_maxed_seq)

        return self._last_seq_number[queue]

    def __get_next_publish_seq_number(self, queue: str) -> int:
        self._last_seq_number.setdefault(queue, 0)
        keys = list(self._last_seq_number.keys())
        keys = [key for key in keys if key.startswith(queue)]

        possible_id = 0
        for i in range(MAX_SEQ_NUMBER):
            if possible_id not in keys:
                break
            possible_id += 1

        if possible_id == MAX_SEQ_NUMBER:
            raise Exception("No more publish ids available")

        for key in keys:
            self._last_seq_number[key] = possible_id

        return possible_id

    def send(self, builder: GenericPacketBuilder, outgoing_messages: Dict[str, Union[List[bytes], Eof]]):
        for (queue, messages_or_eof) in outgoing_messages.items():
            if isinstance(messages_or_eof, Eof) or len(messages_or_eof) > 0:
                if queue.startswith("publish_"):
                    queue = queue[len("publish_"):]
                    encoded = builder.build(self.__get_next_publish_seq_number(queue), messages_or_eof).encode()
                    logging.debug(
                        f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.send_to_route("publish", queue, encoded)
                else:
                    encoded = builder.build(self.__get_next_seq_number(queue), messages_or_eof).encode()
                    logging.debug(
                        f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.produce(queue, encoded)

    def get_state(self) -> bytes:
        state = {
            "last_seq_number": self._last_seq_number
        }
        return pickle.dumps(state)

    def set_state(self, state_bytes: bytes):
        state = pickle.loads(state_bytes)
        self._last_seq_number = state["last_seq_number"]
