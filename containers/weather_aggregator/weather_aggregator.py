#!/usr/bin/env python3
import logging
import os
from datetime import timedelta
from typing import List, Dict, Union

from common.basic_aggregator import BasicAggregator
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.gateway_in import GatewayIn
from common.packets.gateway_out import GatewayOut
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.utils import initialize_log, parse_date, datetime_str_to_date_str

SIDE_TABLE_QUEUE_NAME = os.environ["SIDE_TABLE_QUEUE_NAME"]
REPLICA_ID = os.environ["REPLICA_ID"]


class WeatherAggregator(BasicAggregator):
    def __init__(self, replica_id: int, side_table_queue_name: str):
        super().__init__(replica_id, side_table_queue_name)
        self._weather = {}
        self._unanswered_packets = {}
        self._stopped_cities = set()

    def handle_side_table_message(self, message: bytes):
        packet = WeatherSideTableInfo.decode(message)
        date = parse_date(packet.date)
        yesterday = (date - timedelta(days=1)).date()
        yesterday = yesterday.strftime("%Y-%m-%d")
        self._weather.setdefault(packet.city_name, {})
        self._weather[packet.city_name][yesterday] = packet.prectot

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        # TODO: Delay EOF if there are unanswered packets for this client
        logging.info(f"action: handle_eof | result: in_progress | city_name: {message.city_name}")

        output_queue = Linker().get_eof_in_queue(self)
        logging.info(f"Sending EOF to {output_queue}")
        return {
            output_queue: [message.encode()]
        }

    def __search_prec_for_date(self, city_name: str, date: str) -> Union[int, None]:
        if city_name not in self._weather:
            return None
        if date not in self._weather[city_name]:
            return None
        return self._weather[city_name][date]

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = GatewayIn.decode(message)
        start_date = datetime_str_to_date_str(packet.start_datetime)

        prectot = self.__search_prec_for_date(packet.city_name, start_date)
        if prectot is None:
            if packet.city_name not in self._stopped_cities:
                self._unanswered_packets.setdefault(packet.city_name, [])
                self._unanswered_packets[packet.city_name].append(packet)
            else:
                logging.warning(f"Could not find weather for city {packet.city_name} and date {start_date}.")
            return {}

        output_packet = GatewayOut(packet.city_name, start_date, packet.start_station_code, packet.end_station_code,
                                   packet.duration_sec, packet.yearid, prectot)

        output_queue = Linker().get_output_queue(self, hashing_key=start_date)
        return {
            output_queue: [output_packet.encode()]
        }

    def handle_stop(self, city_name: str) -> Dict[str, List[bytes]]:
        self._stopped_cities.add(city_name)
        output_messages = {}
        for packet in self._unanswered_packets.get(city_name, []):
            new_output_messages = self.handle_message(packet.encode())
            for queue_name, messages in new_output_messages.items():
                output_messages.setdefault(queue_name, [])
                output_messages[queue_name].extend(messages)
        if city_name in self._unanswered_packets:
            del self._unanswered_packets[city_name]
        return output_messages


def main():
    initialize_log(logging.INFO)
    aggregator = WeatherAggregator(int(REPLICA_ID), SIDE_TABLE_QUEUE_NAME)
    aggregator.start()


if __name__ == "__main__":
    main()
