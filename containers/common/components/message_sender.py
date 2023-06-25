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
        logging.info(f"Getting next seq number for {queue} - self._last_seq_number: {self._last_seq_number}")
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
        logging.info(f"Getting next publish seq number for {queue}")
        self._last_seq_number.setdefault(queue, 0)
        logging.info(f"Last seq number: {self._last_seq_number}")
        keys = list(self._last_seq_number.keys())
        logging.info(f"Keys before filtering: {keys}")
        keys = [key for key in keys if key.startswith(queue)]
        used_ids = [self._last_seq_number[key] for key in keys]
        logging.info(f"Keys after filtering: {keys}")

        possible_id = 0
        for i in range(MAX_SEQ_NUMBER):
            if possible_id not in used_ids:
                break
            possible_id += 1

        logging.info(f"Possible id: {possible_id}")

        if possible_id == MAX_SEQ_NUMBER:
            raise Exception("No more publish ids available")

        for key in keys:
            self._last_seq_number[key] = possible_id

        logging.info(f"Last seq number after change: {self._last_seq_number}")

        return possible_id

    def send(self, builder: GenericPacketBuilder, outgoing_messages: Dict[str, Union[List[bytes], Eof]],
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

    def get_state(self) -> bytes:
        state = {
            "last_seq_number": self._last_seq_number
        }
        return pickle.dumps(state)

    def set_state(self, state_bytes: bytes):
        state = pickle.loads(state_bytes)
        self._last_seq_number = state["last_seq_number"]
