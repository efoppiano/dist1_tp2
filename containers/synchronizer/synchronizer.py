#!/usr/bin/env python3
import logging
from dataclasses import dataclass
from typing import Dict, List

import yaml

from common.basic_synchronizer import BasicSynchronizer
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket
from common.utils import initialize_log

from common.utils import build_prefixed_queue_name


class Synchronizer(BasicSynchronizer):
    def __init__(self, config: dict):
        input_queues = [eof_input for eof_input in config]
        super().__init__(input_queues)
        self._config = config
        self._eofs_received = {}
        logging.info(f"action: synchronizer_init | status: success | input_queues: {input_queues}")

    def handle_message(self, queue: str, message: Eof) -> Dict[str, List[bytes]]:
        logging.info(f"Received EOF from {queue} - {message.city_name}")
        output = {}

        city_name = message.city_name
        self._config[queue]["eofs_to_wait"][city_name] -= 1
        if self._config[queue]["eofs_to_wait"][city_name] == 0:
            if "prefixed" in self._config[queue] and self._config[queue]["prefixed"]:
                eof_output_queue_suffix = self._config[queue]["eof_output"]
                eof_output_queue = build_prefixed_queue_name(city_name, eof_output_queue_suffix)
            else:
                eof_output_queue = self._config[queue]["eof_output"]
            logging.info(f"Sending EOF to {eof_output_queue}")
            output[eof_output_queue] = [GenericPacket(Eof(city_name)).encode()]

        return output


def main():
    initialize_log(logging.INFO)
    with open("/opt/app/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    synchronizer = Synchronizer(config)
    synchronizer.start()


if __name__ == "__main__":
    main()
