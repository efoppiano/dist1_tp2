#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class TripsCounter(BasicFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

        self._buffer = {}

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        city_name = message.city_name
        output = {}
        self._buffer.setdefault(city_name, {})
        for start_station_name, data in self._buffer[city_name].items():
            if data[2016] == 0:
                continue
            queue_name = Linker().get_output_queue(self, hashing_key=start_station_name)
            output.setdefault(queue_name, [])
            output[queue_name].append(TripsCountByYearJoined(city_name,
                                                             start_station_name,
                                                             data[2016],
                                                             data[2017]).encode())

        self._buffer.pop(city_name)
        eof_output_queue = Linker().get_eof_in_queue(self)
        output[eof_output_queue] = [message.encode()]
        return output

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = YearFilterIn.decode(message)

        city_name = packet.city_name
        start_station_name = packet.start_station_name
        yearid = packet.yearid
        self._buffer.setdefault(city_name, {})
        self._buffer[city_name].setdefault(start_station_name, {2016: 0, 2017: 0})
        self._buffer[city_name][start_station_name][yearid] += 1

        return {}


def main():
    initialize_log(logging.INFO)
    filter = TripsCounter(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
