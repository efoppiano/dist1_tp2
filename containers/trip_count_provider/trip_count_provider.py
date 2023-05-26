#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.utils import initialize_log, build_eof_in_queue_name

REPLICA_ID = os.environ["REPLICA_ID"]
MULT_THRESHOLD = os.environ["MULT_THRESHOLD"]


class TripCountProvider(BasicFilter):
    def __init__(self, replica_id: int, mult_threshold: float):
        super().__init__(replica_id)
        self._mult_threshold = mult_threshold
        self._replica_id = replica_id

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_output_queue = Linker().get_eof_in_queue(self)
        return {
            eof_output_queue: [EofWithId(message.city_name, self._replica_id).encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = TripsCountByYearJoined.decode(message)

        output = {}
        if packet.trips_17 > self._mult_threshold * packet.trips_16:
            output_queue = Linker().get_output_queue(self)
            output[output_queue] = [message]

        return output


def main():
    initialize_log(logging.INFO)

    filter = TripCountProvider(int(REPLICA_ID), float(MULT_THRESHOLD))
    filter.start()


if __name__ == "__main__":
    main()
