#!/usr/bin/env python3
import json
from typing import Dict, List

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.components.message_sender import OutgoingMessages
from common.packets.dist_info import DistInfo
from common.packets.eof import Eof
from common.packets.station_dist_mean import StationDistMean
from common.utils import initialize_log


class DistMeanCalculator(BasicStatefulFilter):
    def __init__(self):
        self._mean_buffer = {}
        super().__init__()

    def handle_eof(self, flow_id, message: Eof) -> OutgoingMessages:
        eof_output_queue = self.router.publish()
        output = {}
        self._mean_buffer.setdefault(flow_id, {})

        if not message.drop:
            for end_station_name, data in self._mean_buffer[flow_id].items():
                queue_name = self.router.route(end_station_name)
                output.setdefault(queue_name, [])
                output[queue_name].append(
                    StationDistMean(end_station_name, data["mean"], data["count"]).encode()
                )

        self._mean_buffer.pop(flow_id)
        output[eof_output_queue] = message
        return OutgoingMessages(output)

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

    def get_state(self) -> dict:
        return {
            "mean_buffer": self._mean_buffer,
            "parent_state": super().get_state()
        }

    def set_state(self, state: dict):
        self._mean_buffer = state["mean_buffer"]
        super().set_state(state["parent_state"])


def main():
    initialize_log()
    filter = DistMeanCalculator()
    filter.start()


if __name__ == "__main__":
    main()
