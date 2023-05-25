#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.station_dist_mean import StationDistMean
from common.utils import initialize_log, build_eof_in_queue_name

REPLICA_ID = os.environ["REPLICA_ID"]
MEAN_THRESHOLD = os.environ["MEAN_THRESHOLD"]


class DistMeanProvider(BasicFilter):
    def __init__(self, replica_id: int, mean_threshold: float):
        super().__init__(replica_id)

        self._output_queue = Linker().get_output_queue(self)
        self._mean_threshold = mean_threshold

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_output_queue = Linker().get_eof_in_queue(self)
        return {
            eof_output_queue: [message.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = StationDistMean.decode(message)

        output = {}
        if packet.dist_mean >= self._mean_threshold:
            output[self._output_queue] = [message]

        return output


def main():
    initialize_log(logging.INFO)
    filter = DistMeanProvider(int(REPLICA_ID), float(MEAN_THRESHOLD))
    filter.start()


if __name__ == "__main__":
    main()
