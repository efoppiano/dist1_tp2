#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_filter import BasicFilter
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]


class YearFilter(BasicFilter):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        output_queue = Linker().get_eof_in_queue(self)
        return {
            output_queue: [message.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = YearFilterIn.decode(message)
        output = {}
        if packet.yearid in [2016, 2017]:
            output_queue = Linker().get_output_queue(self, hashing_key=packet.start_station_name)
            output[output_queue] = [message]

        return output


def main():
    initialize_log(logging.INFO)
    filter = YearFilter(int(REPLICA_ID))
    filter.start()


if __name__ == "__main__":
    main()
