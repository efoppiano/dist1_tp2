#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.prec_filter_in import PrecFilterIn
from common.utils import initialize_log

PREC_LIMIT = os.environ["PREC_LIMIT"]
REPLICA_ID = os.environ["REPLICA_ID"]


class PrecFilter(BasicFilter):
    def __init__(self, replica_id: int, prec_limit: int):
        super().__init__(replica_id)
        self._prec_limit = prec_limit

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_output_queue = Linker().get_eof_in_queue(self)
        return {
            eof_output_queue: [message.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = PrecFilterIn.decode(message)

        output = {}
        if packet.prectot > self._prec_limit:
            output_queue = Linker().get_output_queue(self, hashing_key=packet.start_date)
            output[output_queue] = [message]

        return output


def main():
    initialize_log(logging.INFO)
    filter = PrecFilter(int(REPLICA_ID), int(PREC_LIMIT))
    filter.start()


if __name__ == "__main__":
    main()
