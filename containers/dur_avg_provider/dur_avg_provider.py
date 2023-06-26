#!/usr/bin/env python3
import pickle
from typing import Dict, List, Union

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.packets.dur_avg_out import DurAvgOut
from common.packets.eof import Eof
from common.packets.prec_filter_in import PrecFilterIn
from common.utils import initialize_log


class DurAvgProvider(BasicStatefulFilter):
    def __init__(self):
        self._avg_buffer = {}
        super().__init__()

    def handle_eof(self, flow_id, message: Eof) -> Dict[str, Union[List[bytes], Eof]]:
        eof_output_queue = self.router.publish()
        self._avg_buffer.setdefault(flow_id, {})
        city_output = []
        if not message.drop:
            for start_date in self._avg_buffer[flow_id]:
                avg = self._avg_buffer[flow_id][start_date]["avg"]
                amount = self._avg_buffer[flow_id][start_date]["count"]
                city_output.append(DurAvgOut(start_date, avg, amount).encode())
        self._avg_buffer.pop(flow_id)
        return {
            self.router.route(): city_output,
            eof_output_queue: message,
        }

    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = PrecFilterIn.decode(message)

        self._avg_buffer.setdefault(flow_id, {})
        self._avg_buffer[flow_id].setdefault(packet.start_date, {"avg": 0, "count": 0})
        old_avg = self._avg_buffer[flow_id][packet.start_date]["avg"]
        old_count = self._avg_buffer[flow_id][packet.start_date]["count"]
        new_count = old_count + 1
        new_avg = (old_avg * old_count + packet.duration_sec) / new_count
        self._avg_buffer[flow_id][packet.start_date]["avg"] = new_avg
        self._avg_buffer[flow_id][packet.start_date]["count"] = new_count

        return {}

    def get_state(self) -> bytes:
        state = {
            "avg_buffer": self._avg_buffer,
            "parent_state": super().get_state()
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._avg_buffer = state["avg_buffer"]
        super().set_state(state["parent_state"])


def main():
    initialize_log()
    filter = DurAvgProvider()
    filter.start()


if __name__ == "__main__":
    main()
