#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.packets.client_response_packets import StationDistMeanOrEof
from common.packets.eof import Eof
from common.packets.station_dist_mean import StationDistMean
from common.utils import initialize_log, build_eof_in_queue_name

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
REPLICA_ID = os.environ["REPLICA_ID"]
MEAN_THRESHOLD = os.environ["MEAN_THRESHOLD"]


class DistMeanProvider(BasicFilter):
    def __init__(self, config: Dict[str, str]):
        super().__init__(config["input_queue"], int(config["replica_id"]))

        self._output_queue = config["output_queue"]
        self._mean_threshold = float(config["mean_threshold"])

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_output_queue = build_eof_in_queue_name(self._output_queue)
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
    filter = DistMeanProvider({
        "input_queue": INPUT_QUEUE,
        "output_queue": OUTPUT_QUEUE,
        "replica_id": REPLICA_ID,
        "mean_threshold": MEAN_THRESHOLD
    })
    filter.start()


if __name__ == "__main__":
    main()
