#!/usr/bin/env python3
import logging
import os
import pickle
from typing import Dict, List

from common.basic_stateful_filter import BasicStatefulFilter
from common.linker.linker import Linker
from common.packets.dist_info import DistInfo
from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.station_dist_mean import StationDistMean
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class DistMeanCalculator(BasicStatefulFilter):
    def __init__(self, replica_id: int):
        self._replica_id = replica_id
        self._mean_buffer = {}
        super().__init__(replica_id)

    def handle_eof(self, flow_id, message: Eof) -> Dict[str, List[bytes]]:
        client_id = message.client_id
        city_name = message.city_name
        eof_output_queue = Linker().get_eof_in_queue(self)
        output = {}
        self._mean_buffer.setdefault(flow_id, {})
        for end_station_name, data in self._mean_buffer[flow_id].items():
            queue_name = Linker().get_output_queue(self, hashing_key=end_station_name)
            output.setdefault(queue_name, [])
            output[queue_name].append(
                StationDistMean(end_station_name,data["mean"]).encode()
            )
        self._mean_buffer.pop(flow_id)
        output[eof_output_queue] = [EofWithId(client_id, city_name, self._replica_id).encode()]
        return output

    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = DistInfo.decode(message)

        self._mean_buffer.setdefault(flow_id, {})
        self._mean_buffer[flow_id].setdefault(packet.end_station_name, {"mean": 0, "count": 0})

        old_mean = self._mean_buffer[flow_id][packet.end_station_name]["mean"]
        old_count = self._mean_buffer[flow_id][packet.end_station_name]["count"]

        new_count = old_count + 1
        new_mean = (old_mean * old_count + packet.distance_km) / new_count

        self._mean_buffer[flow_id][packet.end_station_name]["mean"] = new_mean
        self._mean_buffer[flow_id][packet.end_station_name]["count"] = new_count

        return {}

    def get_state(self) -> bytes:
        state = {
            "mean_buffer": self._mean_buffer
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._mean_buffer = state["mean_buffer"]


def main():
    initialize_log()
    filter = DistMeanCalculator(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
