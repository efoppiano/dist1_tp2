#!/usr/bin/env python3
import os
from typing import Dict, List

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.packets.prec_filter_in import PrecFilterIn
from common.utils import initialize_log

PREC_LIMIT = os.environ["PREC_LIMIT"]
REPLICA_ID = os.environ["REPLICA_ID"]


class PrecFilter(BasicStatefulFilter):
    def __init__(self, replica_id: int, prec_limit: int):
        super().__init__(replica_id)
        self._prec_limit = prec_limit
        self._replica_id = replica_id

    def handle_message(self, _flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = PrecFilterIn.decode(message)

        output = {}
        if packet.prectot > self._prec_limit:
            output_queue = self.router.route(packet.start_date)
            output[output_queue] = [message]

        return output


def main():
    initialize_log()
    filter = PrecFilter(int(REPLICA_ID), int(PREC_LIMIT))
    filter.start()


if __name__ == "__main__":
    main()
