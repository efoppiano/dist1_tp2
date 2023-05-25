#!/usr/bin/env python3
import logging
import os
from typing import Dict, List
from haversine import haversine

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.dist_info import DistInfo
from common.packets.distance_calc_in import DistanceCalcIn
from common.packets.eof import Eof
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class DistanceCalculator(BasicFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_output_queue = Linker().get_eof_in_queue(self)
        return {
            eof_output_queue: [message.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = DistanceCalcIn.decode(message)

        output_queue = Linker().get_output_queue(self, hashing_key=packet.end_station_name)
        distance = self.__calculate_distance(packet.start_station_latitude,
                                             packet.start_station_longitude,
                                             packet.end_station_latitude,
                                             packet.end_station_longitude)
        return {
            output_queue: [DistInfo(packet.city_name,
                                    packet.end_station_name,
                                    distance).encode()]
        }

    @staticmethod
    def __calculate_distance(start_station_latitude: float, start_station_longitude: float,
                             end_station_latitude: float, end_station_longitude: float) -> float:
        return haversine((start_station_latitude, start_station_longitude),
                         (end_station_latitude, end_station_longitude))


def main():
    initialize_log(logging.INFO)
    filter = DistanceCalculator(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
