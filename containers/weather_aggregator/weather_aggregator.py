#!/usr/bin/env python3
import logging
import os
import pickle
from datetime import timedelta
from typing import List, Dict, Union

from common.basic_aggregator import BasicAggregator
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.gateway_in import GatewayIn
from common.packets.gateway_in_or_weather import GatewayInOrWeather
from common.packets.gateway_out import GatewayOut
from common.packets.gateway_out_or_station import GatewayOutOrStation
from common.packets.stop_packet import StopPacket
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.utils import initialize_log, parse_date, datetime_str_to_date_str

SIDE_TABLE_ROUTING_KEY = os.environ["SIDE_TABLE_ROUTING_KEY"]
REPLICA_ID = os.environ["REPLICA_ID"]


class WeatherAggregator(BasicAggregator):
    def __init__(self, replica_id: int, side_table_routing_key: str):
        self._replica_id = replica_id

        self._weather = {}
        super().__init__(replica_id, side_table_routing_key)

    def __handle_side_table_message(self, packet: WeatherSideTableInfo):
        date = parse_date(packet.date)
        yesterday = (date - timedelta(days=1)).date()
        yesterday = yesterday.strftime("%Y-%m-%d")
        self._weather.setdefault(packet.city_name, {})
        self._weather[packet.city_name][yesterday] = packet.prectot

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        client_id = message.client_id
        city_name = message.city_name
        logging.info(f"action: handle_eof | result: in_progress | city_name: {city_name}")

        output_queue = Linker().get_eof_in_queue(self)
        return {
            output_queue: [EofWithId(client_id, city_name, self._replica_id).encode()]
        }

    def __search_prec_for_date(self, city_name: str, date: str) -> Union[int, None]:
        if city_name not in self._weather:
            return None
        if date not in self._weather[city_name]:
            return None
        return self._weather[city_name][date]

    def __handle_gateway_in(self, packet: GatewayIn) -> Dict[str, List[bytes]]:
        start_date = datetime_str_to_date_str(packet.start_datetime)

        prectot = self.__search_prec_for_date(packet.city_name, start_date)
        if prectot is None:
            logging.warning(f"Could not find weather for city {packet.city_name} and date {start_date}.")
            return {}

        output_packet = GatewayOutOrStation(
            GatewayOut(packet.trip_id, packet.city_name, start_date, packet.start_station_code,
                       packet.end_station_code,
                       packet.duration_sec, packet.yearid, prectot))

        output_queue = Linker().get_output_queue(self, hashing_key=start_date)
        return {
            output_queue: [output_packet.encode()]
        }

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = GatewayInOrWeather.decode(message)
        if isinstance(packet.data, GatewayIn):
            return self.__handle_gateway_in(packet.data)
        elif isinstance(packet.data, WeatherSideTableInfo):
            self.__handle_side_table_message(packet.data)
            return {}
        elif isinstance(packet.data, StopPacket):
            return self.__handle_stop(packet.data.city_name)
        else:
            raise ValueError(f"Unknown packet type: {packet}")

    @staticmethod
    def __handle_stop(_city_name: str) -> Dict[str, List[bytes]]:
        return {}

    def get_state(self) -> bytes:
        state = {
            "weather": self._weather,
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._weather = state["weather"]


def main():
    initialize_log(logging.INFO)
    aggregator = WeatherAggregator(int(REPLICA_ID), SIDE_TABLE_ROUTING_KEY)
    aggregator.start()


if __name__ == "__main__":
    main()
