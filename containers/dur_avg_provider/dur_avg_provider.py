#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.dur_avg_out import DurAvgOut
from common.packets.eof import Eof
from common.packets.prec_filter_in import PrecFilterIn
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class DurAvgProvider(BasicFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

        self._output_queue = Linker().get_output_queue(self)
        self._buffer = {}

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        logging.info(f"Received EOF for city {message.city_name}")
        city_name = message.city_name
        eof_output_queue = Linker().get_eof_in_queue(self)
        self._buffer.setdefault(city_name, {})
        city_output = []
        for start_date in self._buffer[city_name]:
            avg = self._buffer[city_name][start_date]["avg"]
            city_output.append(DurAvgOut(city_name, start_date, avg).encode())
        self._buffer.pop(city_name)
        return {
            self._output_queue: city_output,
            eof_output_queue: [message.encode()],
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = PrecFilterIn.decode(message)

        self._buffer.setdefault(packet.city_name, {})
        self._buffer[packet.city_name].setdefault(packet.start_date, {"avg": 0, "count": 0})
        old_avg = self._buffer[packet.city_name][packet.start_date]["avg"]
        old_count = self._buffer[packet.city_name][packet.start_date]["count"]
        new_count = old_count + 1
        new_avg = (old_avg * old_count + packet.duration_sec) / new_count
        self._buffer[packet.city_name][packet.start_date]["avg"] = new_avg
        self._buffer[packet.city_name][packet.start_date]["count"] = new_count

        return {}


def main():
    initialize_log(logging.INFO)
    filter = DurAvgProvider(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
