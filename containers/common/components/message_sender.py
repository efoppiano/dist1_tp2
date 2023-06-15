import logging
from typing import Dict, Union, List

from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacketBuilder
from common.middleware.rabbit_middleware import Rabbit
from common.utils import min_hash, log_msg

MAX_SEQ_NUMBER = 2 ** 9  # 2 packet ids would be enough, but we use more for traceability
class MessageSender:
    def __init__(self, middleware: Rabbit):
        self._last_seq_number = 0
        self._rabbit = middleware

        self._times_maxed_seq = 0

    def __get_next_seq_number(self) -> int:
        self._last_seq_number = (self._last_seq_number + 1)

        if self._last_seq_number > MAX_SEQ_NUMBER:
            self._last_seq_number = 0
            self._times_maxed_seq += 1
            log_msg("Generated %d packets [%d]", MAX_SEQ_NUMBER, self._times_maxed_seq)
            

        return self._last_seq_number

    def send(self, builder: GenericPacketBuilder, outgoing_messages: Dict[str, Union[List[bytes], Eof]]):
        for (queue, messages_or_eof) in outgoing_messages.items():
            if isinstance(messages_or_eof, Eof) or len(messages_or_eof) > 0:
                encoded = builder.build(self.__get_next_seq_number(), messages_or_eof).encode()
                if queue.startswith("publish_"):
                    queue = queue[len("publish_"):]
                    logging.debug(
                        f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.send_to_route("publish", queue, encoded)
                else:
                    logging.debug(
                        f"Sending {builder.get_id()}-{min_hash(messages_or_eof)} to {queue}")
                    self._rabbit.produce(queue, encoded)

    def get_state(self) -> bytes:
        return self._last_seq_number.to_bytes(4, "big")

    def set_state(self, state: bytes):
        self._last_seq_number = int.from_bytes(state, "big")
