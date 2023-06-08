#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_stateful_filter import BasicStatefulFilter
from common.linker.linker import Linker
from common.packets.station_dist_mean import StationDistMean
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]
MEAN_THRESHOLD = os.environ["MEAN_THRESHOLD"]


class DistMeanProvider(BasicStatefulFilter):
    def __init__(self, replica_id: int, mean_threshold: float):
        super().__init__(replica_id)

        self._replica_id = replica_id
        self._output_queue = Linker().get_output_queue(self)
        self._mean_threshold = mean_threshold

    def handle_message(self, _flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = StationDistMean.decode(message)

        output = {}
        if packet.dist_mean >= self._mean_threshold:
            output[self._output_queue] = [message]

        return output


def main():
    initialize_log()
    filter = DistMeanProvider(int(REPLICA_ID), float(MEAN_THRESHOLD))
    filter.start()


if __name__ == "__main__":
    main()
