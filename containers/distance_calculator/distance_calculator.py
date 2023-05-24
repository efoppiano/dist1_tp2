#!/usr/bin/env python3
import logging
import os
from typing import Dict, List
from haversine import haversine

from common.basic_filter import BasicFilter
from common.packets.dist_info import DistInfo
from common.packets.distance_calc_in import DistanceCalcIn
from common.packets.eof import Eof
from common.utils import initialize_log, build_hashed_queue_name, build_eof_in_queue_name

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
OUTPUT_AMOUNT = os.environ["OUTPUT_AMOUNT"]
REPLICA_ID = os.environ["REPLICA_ID"]


class DistanceCalculator(BasicFilter):
    def __init__(self, config: Dict[str, str]):
        super().__init__(config["input_queue"], int(config["replica_id"]))

        self._output_queue = config["output_queue"]
        self._output_amount = int(config["output_amount"])

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_output_queue = build_eof_in_queue_name(self._output_queue)
        return {
            eof_output_queue: [message.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = DistanceCalcIn.decode(message)

        output_queue = build_hashed_queue_name(self._output_queue,
                                               packet.end_station_name,
                                               self._output_amount)
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
    filter = DistanceCalculator({
        "input_queue": INPUT_QUEUE,
        "output_queue": OUTPUT_QUEUE,
        "output_amount": OUTPUT_AMOUNT,
        "replica_id": REPLICA_ID
    })
    filter.start()


if __name__ == "__main__":
    main()
