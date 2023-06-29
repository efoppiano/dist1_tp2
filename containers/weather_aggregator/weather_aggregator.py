#!/usr/bin/env python3
import os
import json
from datetime import timedelta
from typing import List, Dict, Union

from common.basic_classes.basic_aggregator import BasicAggregator
from common.components.message_sender import OutgoingMessages
from common.packets.eof import Eof
from common.packets.gateway_in import GatewayIn
from common.packets.gateway_in_or_weather import GatewayInOrWeather
from common.packets.gateway_out import GatewayOut
from common.packets.gateway_out_or_station import GatewayOutOrStation
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.router import MultiRouter
from common.utils import initialize_log, parse_date, datetime_str_to_date_str, log_missing

NEXT = os.environ["NEXT"]
NEXT_AMOUNT = int(os.environ["NEXT_AMOUNT"])


class WeatherAggregator(BasicAggregator):
    def __init__(self, router: MultiRouter):
        self._weather = {}
        super().__init__(router)

    def handle_eof(self, flow_id, message: Eof) -> Dict[str, Eof]:
        self._weather.pop(flow_id, None)
        return super().handle_eof(flow_id, message)

    def __handle_side_table_message(self, flow_id: str, packet: WeatherSideTableInfo):
        date = parse_date(packet.date)
        yesterday = (date - timedelta(days=1)).date()
        yesterday = yesterday.strftime("%Y-%m-%d")
        self._weather.setdefault(flow_id, {})
        self._weather[flow_id][yesterday] = packet.prectot

    def __search_prec_for_date(self, flow_id: str, date: str) -> Union[int, None]:
        if flow_id not in self._weather:
            return None
        return self._weather[flow_id].get(date, None)

    def __handle_gateway_in(self, flow_id: str, packet: GatewayIn) -> OutgoingMessages:
        start_date = datetime_str_to_date_str(packet.start_datetime)

        prectot = self.__search_prec_for_date(flow_id, start_date)
        if prectot is None:
            log_missing(f"Could not find weather for city {flow_id} and date {start_date}.")
            return OutgoingMessages({})

        output_packet = GatewayOutOrStation(
            GatewayOut(
                start_date, packet.start_station_code, packet.end_station_code,
                packet.duration_sec, packet.yearid, prectot
            )
        )

        output_queue = self.router.route("next", start_date)
        return OutgoingMessages({
            output_queue: [output_packet.encode()]
        })

    def handle_message(self, flow_id: str, message: bytes) -> OutgoingMessages:
        packet = GatewayInOrWeather.decode(message)
        if isinstance(packet.data, GatewayIn):
            return self.__handle_gateway_in(flow_id, packet.data)
        elif isinstance(packet.data, WeatherSideTableInfo):
            self.__handle_side_table_message(flow_id, packet.data)
            return OutgoingMessages({})
        else:
            raise ValueError(f"Unknown packet type: {type(packet.data)}")

    @staticmethod
    def __handle_stop(_flow_id) -> Dict[str, List[bytes]]:
        return {}

    def get_state(self) -> dict:
        state = {
            "weather": self._weather,
            "parent_state": super().get_state()
        }
        return state

    def set_state(self, state: dict):
        self._weather = state["weather"]
        super().set_state(state["parent_state"])


def main():
    initialize_log()
    router = MultiRouter({"next": (NEXT, NEXT_AMOUNT)})
    aggregator = WeatherAggregator(router)
    aggregator.start()


if __name__ == "__main__":
    main()
