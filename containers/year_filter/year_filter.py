#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log


class YearFilter(BasicStatefulFilter):
    def handle_message(self, _flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = YearFilterIn.decode(message)
        output = {}
        if packet.yearid in [2016, 2017]:
            output_queue = self.router.route(packet.start_station_name)
            output[output_queue] = [message]

        return output


def main():
    initialize_log()
    filter = YearFilter()
    filter.start()


if __name__ == "__main__":
    main()
