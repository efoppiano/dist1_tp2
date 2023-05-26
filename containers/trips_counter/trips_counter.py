#!/usr/bin/env python3
import logging
import os
import pickle
from typing import Dict, List

from common.basic_stateful_filter import BasicStatefulFilter
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class TripsCounter(BasicStatefulFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

        self._replica_id = replica_id
        self._count_buffer = {}

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        city_name = message.city_name
        output = {}
        self._count_buffer.setdefault(city_name, {})
        for start_station_name, data in self._count_buffer[city_name].items():
            if data[2016] == 0:
                continue
            queue_name = Linker().get_output_queue(self, hashing_key=start_station_name)
            output.setdefault(queue_name, [])
            output[queue_name].append(TripsCountByYearJoined(data.id,
                                                             city_name,
                                                             start_station_name,
                                                             data[2016],
                                                             data[2017]).encode())

        self._count_buffer.pop(city_name)
        eof_output_queue = Linker().get_eof_in_queue(self)
        output[eof_output_queue] = [EofWithId(city_name, self._replica_id).encode()]
        return output

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = YearFilterIn.decode(message)

        city_name = packet.city_name
        start_station_name = packet.start_station_name
        yearid = packet.yearid
        self._count_buffer.setdefault(city_name, {})
        # TODO: check if the id could cause problems
        self._count_buffer[city_name].setdefault(start_station_name, {2016: 0, 2017: 0, "id": packet.trip_id})
        self._count_buffer[city_name][start_station_name][yearid] += 1

        return {}

    def get_state(self) -> bytes:
        state = {
            "count_buffer": self._count_buffer,
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._count_buffer = state["count_buffer"]


def main():
    initialize_log(logging.INFO)
    filter = TripsCounter(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
