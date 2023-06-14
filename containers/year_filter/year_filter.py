#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class YearFilter(BasicStatefulFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

        self._replica_id = replica_id

    def handle_message(self, _flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = YearFilterIn.decode(message)
        output = {}
        if packet.yearid in [2016, 2017]:
            output_queue = self.router.route(packet.start_station_name)
            output[output_queue] = [message]

        return output


def main():
    initialize_log()
    filter = YearFilter(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
