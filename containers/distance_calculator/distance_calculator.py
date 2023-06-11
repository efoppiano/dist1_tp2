#!/usr/bin/env python3
import logging
import os
from typing import Dict, List
from haversine import haversine

from common.basic_stateful_filter import BasicStatefulFilter
from common.packets.dist_info import DistInfo
from common.packets.distance_calc_in import DistanceCalcIn
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class DistanceCalculator(BasicStatefulFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

        self._replica_id = replica_id

    def handle_message(self, _flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = DistanceCalcIn.decode(message)

        output_queue = self.router.route(packet.end_station_name)
        distance = self.__calculate_distance(packet.start_station_latitude,
                                             packet.start_station_longitude,
                                             packet.end_station_latitude,
                                             packet.end_station_longitude)
        return {
            output_queue: [DistInfo(packet.end_station_name, distance).encode()]
        }

    @staticmethod
    def __calculate_distance(start_station_latitude: float, start_station_longitude: float,
                             end_station_latitude: float, end_station_longitude: float) -> float:
        return haversine((start_station_latitude, start_station_longitude),
                         (end_station_latitude, end_station_longitude))


def main():
    initialize_log()
    filter = DistanceCalculator(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
