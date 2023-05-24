#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.packets.eof import Eof
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log, build_hashed_queue_name, build_eof_in_queue_name

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
OUTPUT_AMOUNT = os.environ["OUTPUT_AMOUNT"]
REPLICA_ID = os.environ["REPLICA_ID"]


class TripsCounter(BasicFilter):
    def __init__(self, config: Dict[str, str]):
        super().__init__(config["input_queue"], int(config["replica_id"]))

        self._output_queue = config["output_queue"]
        self._output_amount = int(config["output_amount"])

        self._buffer = {}

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        city_name = message.city_name
        output = {}
        self._buffer.setdefault(city_name, {})
        for start_station_name, data in self._buffer[city_name].items():
            if data[2016] == 0:
                continue
            queue = build_hashed_queue_name(self._output_queue,
                                            start_station_name,
                                            self._output_amount)
            output.setdefault(queue, [])
            output[queue].append(TripsCountByYearJoined(city_name,
                                                        start_station_name,
                                                        data[2016],
                                                        data[2017]).encode())

        self._buffer.pop(city_name)
        eof_output_queue = build_eof_in_queue_name(self._output_queue)
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
    filter = TripsCounter({
        "input_queue": INPUT_QUEUE,
        "output_queue": OUTPUT_QUEUE,
        "output_amount": OUTPUT_AMOUNT,
        "replica_id": REPLICA_ID,
    })
    filter.start()


if __name__ == "__main__":
    main()
