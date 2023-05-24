#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.packets.eof import Eof
from common.packets.prec_filter_in import PrecFilterIn
from common.utils import initialize_log, build_hashed_queue_name, build_eof_in_queue_name

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
OUTPUT_AMOUNT = os.environ["OUTPUT_AMOUNT"]
PREC_LIMIT = os.environ["PREC_LIMIT"]
REPLICA_ID = os.environ["REPLICA_ID"]


class PrecFilter(BasicFilter):
    def __init__(self, config: Dict[str, str]):
        super().__init__(config["input_queue"], int(config["replica_id"]))
        self._output_queue = config["output_queue"]
        self._prec_limit = int(config["prec_limit"])
        self._output_amount = int(config["output_amount"])

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_output_queue = build_eof_in_queue_name(self._output_queue)
        return {
            eof_output_queue: [message.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = PrecFilterIn.decode(message)

        output = {}
        if packet.prectot > self._prec_limit:
            output_queue = build_hashed_queue_name(self._output_queue,
                                                   packet.start_date, self._output_amount)
            output[output_queue] = [message]

        return output


def main():
    initialize_log(logging.INFO)
    filter = PrecFilter({
        "input_queue": INPUT_QUEUE,
        "output_queue": OUTPUT_QUEUE,
        "output_amount": OUTPUT_AMOUNT,
        "prec_limit": PREC_LIMIT,
        "replica_id": REPLICA_ID
    })
    filter.start()


if __name__ == "__main__":
    main()
