#!/usr/bin/env python3
import os
from typing import Dict, List

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter
from common.packets.station_dist_mean import StationDistMean
from common.utils import initialize_log

MEAN_THRESHOLD = os.environ["MEAN_THRESHOLD"]


class DistMeanProvider(BasicStatefulFilter):
    def __init__(self, mean_threshold: float):
        super().__init__()
        self._output_queue = self.router.route()
        self._mean_threshold = mean_threshold

    def handle_message(self, _flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = StationDistMean.decode(message)

        output = {}
        if packet.dist_mean >= self._mean_threshold:
            output[self._output_queue] = [message]

        return output


def main():
    initialize_log()
    filter = DistMeanProvider(float(MEAN_THRESHOLD))
    filter.start()


if __name__ == "__main__":
    main()
