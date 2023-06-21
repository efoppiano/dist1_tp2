#!/usr/bin/env python3
import os
from typing import Dict, List

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.utils import initialize_log

MULT_THRESHOLD = os.environ["MULT_THRESHOLD"]


class TripCountProvider(BasicStatefulFilter):
    def __init__(self, mult_threshold: float):
        super().__init__()
        self._mult_threshold = mult_threshold

    def handle_message(self, _flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = TripsCountByYearJoined.decode(message)

        output = {}
        if packet.trips_17 >= self._mult_threshold * packet.trips_16:
            output_queue = self.router.route()
            output[output_queue] = [message]

        return output


def main():
    initialize_log()
    filter = TripCountProvider(float(MULT_THRESHOLD))
    filter.start()


if __name__ == "__main__":
    main()
