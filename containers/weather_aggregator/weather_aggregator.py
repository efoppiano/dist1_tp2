#!/usr/bin/env python3
import logging
import os
from datetime import timedelta
from typing import List, Dict

from common.basic_aggregator import BasicAggregator
from common.packets.eof import Eof
from common.packets.gateway_in import GatewayIn
from common.packets.gateway_out import GatewayOut
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.utils import initialize_log, build_prefixed_queue_name, build_prefixed_hashed_queue_name, \
    build_eof_in_queue_name, parse_date, datetime_str_to_date_str

CITY_NAME = os.environ["CITY_NAME"]
INPUT_QUEUE_SUFFIX = os.environ["INPUT_QUEUE_SUFFIX"]
SIDE_TABLE_QUEUE_SUFFIX = os.environ["SIDE_TABLE_QUEUE_SUFFIX"]
OUTPUT_QUEUE_SUFFIX = os.environ["OUTPUT_QUEUE_SUFFIX"]
REPLICA_ID = os.environ["REPLICA_ID"]
OUTPUT_AMOUNT = os.environ["OUTPUT_AMOUNT"]


class WeatherAggregator(BasicAggregator):

    def __init__(self, config: Dict[str, str]):
        self._city_name = config["city_name"]
        replica_id = int(config["replica_id"])
        side_table_queue = build_prefixed_queue_name(self._city_name, config["side_table_queue_suffix"])
        super().__init__(self._city_name, config["input_queue_suffix"], replica_id, side_table_queue)

        self._output_queue_suffix = config["output_queue_suffix"]
        self._output_amount = int(config["output_amount"])

        self._weather = {}

    def handle_side_table_message(self, message: bytes):
        packet = WeatherSideTableInfo.decode(message)
        date = parse_date(packet.date)
        yesterday = (date - timedelta(days=1)).date()
        yesterday = yesterday.strftime("%Y-%m-%d")
        self._weather[yesterday] = packet.prectot

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        logging.info("Received EOF")
        output_queue = build_eof_in_queue_name(self._city_name, self._output_queue_suffix)
        return {
            output_queue: [message.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = GatewayIn.decode(message)
        start_date = datetime_str_to_date_str(packet.start_datetime)

        if start_date in self._weather:
            output_packet = GatewayOut(start_date, packet.start_station_code, packet.end_station_code,
                                       packet.duration_sec, packet.yearid, self._weather[start_date])

            output_queue = build_prefixed_hashed_queue_name(self._city_name, self._output_queue_suffix,
                                                            start_date,
                                                            self._output_amount)
            return {
                output_queue: [output_packet.encode()]
            }
        else:
            logging.warning(f"Could not find weather for {start_date}.")
            return {}


def main():
    initialize_log(logging.INFO)
    aggregator = WeatherAggregator({
        "city_name": CITY_NAME,
        "input_queue_suffix": INPUT_QUEUE_SUFFIX,
        "output_queue_suffix": OUTPUT_QUEUE_SUFFIX,
        "replica_id": REPLICA_ID,
        "output_amount": OUTPUT_AMOUNT,
        "side_table_queue_suffix": SIDE_TABLE_QUEUE_SUFFIX,
    })
    aggregator.start()


if __name__ == "__main__":
    main()
