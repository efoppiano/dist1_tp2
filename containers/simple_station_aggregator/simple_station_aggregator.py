#!/usr/bin/env python3
import logging
import os
from typing import Dict, List

from common.basic_aggregator import BasicAggregator
from common.packets.eof import Eof
from common.packets.gateway_out import GatewayOut
from common.packets.basic_station_side_table_info import BasicStationSideTableInfo
from common.packets.generic_packet import GenericPacket
from common.packets.prec_filter_in import PrecFilterIn
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log, build_prefixed_queue_name, build_hashed_queue_name, build_eof_in_queue_name

CITY_NAME = os.environ["CITY_NAME"]
INPUT_QUEUE_SUFFIX = os.environ["INPUT_QUEUE_SUFFIX"]
PREC_FILTER_IN_QUEUE_NAME = os.environ["PREC_FILTER_IN_QUEUE_NAME"]
PREC_FILTER_AMOUNT = os.environ["PREC_FILTER_AMOUNT"]
YEAR_FILTER_IN_QUEUE_NAME = os.environ["YEAR_FILTER_IN_QUEUE_NAME"]
YEAR_FILTER_AMOUNT = os.environ["YEAR_FILTER_AMOUNT"]
SIDE_TABLE_QUEUE_SUFFIX = os.environ["SIDE_TABLE_QUEUE_SUFFIX"]
REPLICA_ID = os.environ["REPLICA_ID"]


class SimpleStationAggregator(BasicAggregator):
    def __init__(self, config: Dict[str, str]):
        self._city_name = config["city_name"]
        replica_id = int(config["replica_id"])
        side_table_queue = build_prefixed_queue_name(config["city_name"], config["side_table_queue_suffix"])
        super().__init__(self._city_name, config["input_queue_suffix"], replica_id, side_table_queue)

        self._prec_filter_in_queue_name = config["prec_filter_in_queue_name"]
        self._prec_filter_amount = int(config["prec_filter_amount"])
        self._year_filter_in_queue_name = config["year_filter_in_queue_name"]
        self._year_filter_amount = int(config["year_filter_amount"])

        self._stations = {}

    def handle_side_table_message(self, message: bytes):
        packet = BasicStationSideTableInfo.decode(message)
        station_code, yearid = packet.station_code, packet.yearid

        self._stations[(station_code, yearid)] = packet.station_name

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        eof_year_filter_in_queue = build_eof_in_queue_name(self._year_filter_in_queue_name)
        eof_prec_filter_in_queue = build_eof_in_queue_name(self._prec_filter_in_queue_name)

        return {
            eof_year_filter_in_queue: [Eof(self._city_name).encode()],
            eof_prec_filter_in_queue: [Eof(self._city_name).encode()]
        }

    def __check_station_name_exists(self, station_code: int, yearid: int) -> bool:
        if (station_code, yearid) not in self._stations:
            logging.warning(f"Station name not found for station code {station_code} and yearid {yearid}")
            return False
        return True

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = GatewayOut.decode(message)

        check_1 = self.__check_station_name_exists(packet.start_station_code,
                                                   packet.yearid)
        check_2 = self.__check_station_name_exists(packet.end_station_code,
                                                   packet.yearid)

        if not check_1 or not check_2:
            return {}

        start_station_name = self._stations[(packet.start_station_code, packet.yearid)]

        prec_filter_in_packet = PrecFilterIn(
            self._city_name, packet.start_date, packet.duration_sec, packet.prectot
        )

        year_filter_in_packet = YearFilterIn(
            self._city_name, start_station_name, packet.yearid
        )

        prec_filter_in_queue = build_hashed_queue_name(self._prec_filter_in_queue_name, str(packet.start_station_code),
                                                       self._prec_filter_amount)
        year_filter_in_queue = build_hashed_queue_name(self._year_filter_in_queue_name, str(packet.start_station_code),
                                                       self._year_filter_amount)
        return {
            prec_filter_in_queue: [prec_filter_in_packet.encode()],
            year_filter_in_queue: [year_filter_in_packet.encode()]
        }


def main():
    initialize_log(logging.INFO)
    aggregator = SimpleStationAggregator({
        "city_name": CITY_NAME,
        "input_queue_suffix": INPUT_QUEUE_SUFFIX,
        "prec_filter_in_queue_name": PREC_FILTER_IN_QUEUE_NAME,
        "prec_filter_amount": 1,
        "year_filter_in_queue_name": YEAR_FILTER_IN_QUEUE_NAME,
        "year_filter_amount": 1,
        "side_table_queue_suffix": SIDE_TABLE_QUEUE_SUFFIX,
        "replica_id": REPLICA_ID
    })
    aggregator.start()


if __name__ == "__main__":
    main()
