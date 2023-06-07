#!/usr/bin/env python3
import logging
import os
import pickle
from typing import Dict, List

from common.basic_stateful_filter import BasicStatefulFilter
from common.linker.linker import Linker
from common.packets.dur_avg_out import DurAvgOut
from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.prec_filter_in import PrecFilterIn
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class DurAvgProvider(BasicStatefulFilter):
    def __init__(self, replica_id: int):
        self._replica_id = replica_id
        self._output_queue = Linker().get_output_queue(self)
        self._avg_buffer = {}
        super().__init__(replica_id)

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        logging.info(f"Received EOF for city {message.city_name}")
        client_id = message.client_id
        city_name = message.city_name
        eof_output_queue = Linker().get_eof_in_queue(self)
        self._avg_buffer.setdefault(city_name, {})
        city_output = []
        for start_date in self._avg_buffer[city_name]:
            avg = self._avg_buffer[city_name][start_date]["avg"]
            amount = self._avg_buffer[city_name][start_date]["count"]
            packet_id = self._avg_buffer[city_name][start_date]["id"]
            city_output.append(DurAvgOut(packet_id, city_name, start_date, avg, amount).encode())
        self._avg_buffer.pop(city_name)
        return {
            self._output_queue: city_output,
            eof_output_queue: [EofWithId(client_id, city_name, self._replica_id).encode()],
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = PrecFilterIn.decode(message)

        self._avg_buffer.setdefault(packet.city_name, {})
        # TODO: check if the id could cause problems
        self._avg_buffer[packet.city_name].setdefault(packet.start_date, {"avg": 0, "count": 0, "id": packet.trip_id})
        old_avg = self._avg_buffer[packet.city_name][packet.start_date]["avg"]
        old_count = self._avg_buffer[packet.city_name][packet.start_date]["count"]
        new_count = old_count + 1
        new_avg = (old_avg * old_count + packet.duration_sec) / new_count
        self._avg_buffer[packet.city_name][packet.start_date]["avg"] = new_avg
        self._avg_buffer[packet.city_name][packet.start_date]["count"] = new_count

        return {}

    def get_state(self) -> bytes:
        state = {
            "avg_buffer": self._avg_buffer,
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._avg_buffer = state["avg_buffer"]


def main():
    initialize_log(logging.INFO)
    filter = DurAvgProvider(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
