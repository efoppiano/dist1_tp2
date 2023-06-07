#!/usr/bin/env python3
import logging
import pickle
from typing import Dict, List

import yaml

from common.basic_synchronizer import BasicSynchronizer
from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.generic_packet import GenericPacket
from common.utils import initialize_log


class Synchronizer(BasicSynchronizer):
    def __init__(self, config: dict):
        input_queues = [eof_input for eof_input in config]
        super().__init__(input_queues)
        self._config = config
        self._eofs_received = {}
        logging.info(f"action: synchronizer_init | status: success | input_queues: {input_queues}")

    def handle_message(self, queue: str, message: EofWithId) -> Dict[str, List[bytes]]:
        logging.info(f"Received EOF from {queue} - {message.city_name}")
        output = {}

        city_name = message.city_name
        self._eofs_received.setdefault(queue, {})
        self._eofs_received[queue].setdefault(city_name, set())
        self._eofs_received[queue][city_name].add(message.replica_id)

        if len(self._eofs_received[queue][city_name]) == self._config[queue]["eofs_to_wait"]:
            eof_output_queue = self._config[queue]["eof_output"]
            logging.info(f"Sending EOF to {eof_output_queue}")
            packet = GenericPacket(
                replica_id=1, 
                # TODO NEXT
                client_id=None,
                city_name=None,
                packet_id=None,
                data= Eof(city_name)
            ).encode()
            output[eof_output_queue] = [packet]

        return output

    def get_state(self) -> bytes:
        state = {
            "eofs_received": self._eofs_received
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._eofs_received = state["eofs_received"]


def main():
    initialize_log(logging.INFO)
    with open("/opt/app/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    synchronizer = Synchronizer(config)
    synchronizer.start()


if __name__ == "__main__":
    main()
