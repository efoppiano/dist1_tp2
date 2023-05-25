#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.dist_info import DistInfo
from common.packets.eof import Eof
from common.packets.station_dist_mean import StationDistMean
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class DistMeanCalculator(BasicFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

        self._buffer = {}

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        city_name = message.city_name
        eof_output_queue = Linker().get_eof_in_queue(self)
        output = {}
        self._buffer.setdefault(city_name, {})
        for end_station_name, data in self._buffer[city_name].items():
            queue_name = Linker().get_output_queue(self, hashing_key=end_station_name)
            output.setdefault(queue_name, [])
            output[queue_name].append(StationDistMean(city_name,
                                                      end_station_name,
                                                      data["mean"]).encode())
        self._buffer.pop(city_name)
        output[eof_output_queue] = [message.encode()]
        return output

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = DistInfo.decode(message)
        city_name = packet.city_name

        self._buffer.setdefault(city_name, {})
        self._buffer[city_name].setdefault(packet.end_station_name, {"mean": 0, "count": 0})

        old_mean = self._buffer[city_name][packet.end_station_name]["mean"]
        old_count = self._buffer[city_name][packet.end_station_name]["count"]

        new_count = old_count + 1
        new_mean = (old_mean * old_count + packet.distance_km) / new_count

        self._buffer[city_name][packet.end_station_name]["mean"] = new_mean
        self._buffer[city_name][packet.end_station_name]["count"] = new_count

        return {}


def main():
    initialize_log(logging.INFO)
    filter = DistMeanCalculator(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
