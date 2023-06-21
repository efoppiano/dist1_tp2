#!/usr/bin/env python3
import pickle
from typing import Dict, List, Union

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.packets.eof import Eof
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log


class TripsCounter(BasicStatefulFilter):
    def __init__(self):
        self._count_buffer = {}
        super().__init__()

    def handle_eof(self, flow_id, message: Eof) -> Dict[str, Union[List[bytes], Eof]]:
        output = {}
        self._count_buffer.setdefault(flow_id, {})
        if not message.drop:
            for start_station_name, data in self._count_buffer[flow_id].items():
                if data[2016] == 0:
                    continue
                queue_name = self.router.route(start_station_name)
                output.setdefault(queue_name, [])
                output[queue_name].append(
                    TripsCountByYearJoined(
                        start_station_name,
                        data[2016],
                        data[2017]
                    ).encode()
                )

        self._count_buffer.pop(flow_id)
        eof_output_queue = self.router.publish()
        output[eof_output_queue] = message
        return output

    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = YearFilterIn.decode(message)

        start_station_name = packet.start_station_name
        yearid = packet.yearid
        self._count_buffer.setdefault(flow_id, {})
        self._count_buffer[flow_id].setdefault(start_station_name, {2016: 0, 2017: 0})
        self._count_buffer[flow_id][start_station_name][yearid] += 1

        return {}

    def get_state(self) -> bytes:
        state = {
            "count_buffer": self._count_buffer,
            "parent_state": super().get_state()
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._count_buffer = state["count_buffer"]
        super().set_state(state["parent_state"])


def main():
    initialize_log()
    filter = TripsCounter()
    filter.start()


if __name__ == "__main__":
    main()
