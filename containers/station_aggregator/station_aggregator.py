#!/usr/bin/env python3
import logging
import os
import pickle
from typing import Dict, List, Union, Tuple

from common.basic_aggregator import BasicAggregator
from common.linker.linker import Linker
from common.packets.distance_calc_in import DistanceCalcIn
from common.packets.eof import Eof
from common.packets.eof_with_id import EofWithId
from common.packets.gateway_out import GatewayOut
from common.packets.gateway_out_or_station import GatewayOutOrStation
from common.packets.prec_filter_in import PrecFilterIn
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.stop_packet import StopPacket
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log

SIDE_TABLE_QUEUE_PREFIX = os.environ["SIDE_TABLE_QUEUE_PREFIX"]
SIDE_TABLE_ROUTING_KEY = os.environ["SIDE_TABLE_ROUTING_KEY"]
REPLICA_ID = os.environ["REPLICA_ID"]


class StationAggregator(BasicAggregator):
    def __init__(self, replica_id: int, side_table_queue_prefix: str, side_table_routing_key: str):
        self._replica_id = replica_id
        self._stations = {}
        super().__init__(replica_id, side_table_queue_prefix, side_table_routing_key)

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        city_name = message.city_name
        logging.info(f"action: handle_eof | result: in_progress | city_name: {city_name}")

        eof_year_filter_queue = Linker().get_eof_in_queue(self, "YearFilter")
        eof_prec_filter_queue = Linker().get_eof_in_queue(self, "PrecFilter")
        eof_distance_calc_queue = Linker().get_eof_in_queue(self, "DistanceCalculator")

        return {
            eof_year_filter_queue: [EofWithId(city_name, self._replica_id).encode()],
            eof_prec_filter_queue: [EofWithId(city_name, self._replica_id).encode()],
            eof_distance_calc_queue: [EofWithId(city_name, self._replica_id).encode()],
        }

    def __handle_side_table_message(self, packet: StationSideTableInfo):
        city_name, station_code, yearid = packet.city_name, packet.station_code, packet.yearid

        self._stations.setdefault(city_name, {})

        self._stations[city_name][f"{station_code}-{yearid}"] = {
            "station_name": packet.station_name,
            "latitude": packet.latitude,
            "longitude": packet.longitude,
        }

    def __search_station(self, city_name: str, station_code: int, yearid: int) -> Union[dict, None]:
        try:
            return self._stations[city_name][f"{station_code}-{yearid}"]
        except KeyError:
            return None

    def __search_stations(self, packet) -> Union[Tuple[dict, dict], None]:
        start_station = self.__search_station(packet.city_name,
                                              packet.start_station_code,
                                              packet.yearid)
        if not start_station:
            return None
        end_station = self.__search_station(packet.city_name,
                                            packet.end_station_code,
                                            packet.yearid)
        if not end_station:
            return None
        return start_station, end_station

    def __build_packet_lists(self, packet: GatewayOut, stations: Tuple[dict, dict]) -> Tuple[
        List[bytes], List[bytes], List[bytes]]:
        start_station_info = stations[0]
        end_station_info = stations[1]

        prec_filter_in_packet = PrecFilterIn(
            packet.trip_id, packet.city_name, packet.start_date, packet.duration_sec, packet.prectot
        )

        year_filter_in_packet = YearFilterIn(
            packet.trip_id, packet.city_name, start_station_info["station_name"], packet.yearid
        )

        if start_station_info["latitude"] is not None:
            distance_calc_in_packet = DistanceCalcIn(
                packet.trip_id,
                packet.city_name,
                start_station_info["station_name"],
                start_station_info["latitude"],
                start_station_info["longitude"],
                end_station_info["station_name"],
                end_station_info["latitude"],
                end_station_info["longitude"],
            )
            distance_calc_in_packets_list = [distance_calc_in_packet.encode()]
        else:
            distance_calc_in_packets_list = []

        return [prec_filter_in_packet.encode()], [year_filter_in_packet.encode()], distance_calc_in_packets_list

    def __handle_gateway_out(self, packet: GatewayOut) -> Dict[str, List[bytes]]:
        stations = self.__search_stations(packet)
        if not stations:
            logging.warning(f"Could not find stations for packet: {packet}")
            return {}

        prec_filter_queue = Linker().get_output_queue(self, "PrecFilter", str(packet.start_station_code))
        year_filter_queue = Linker().get_output_queue(self, "YearFilter", str(packet.start_station_code))
        distance_calc_queue = Linker().get_output_queue(self, "DistanceCalculator", str(packet.start_station_code))

        output_packets_lists = self.__build_packet_lists(packet, stations)

        output = {
            prec_filter_queue: output_packets_lists[0],
            year_filter_queue: output_packets_lists[1],
            distance_calc_queue: output_packets_lists[2],
        }
        return output

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = GatewayOutOrStation.decode(message)
        if isinstance(packet.data, GatewayOut):
            return self.__handle_gateway_out(packet.data)
        elif isinstance(packet.data, StationSideTableInfo):
            self.__handle_side_table_message(packet.data)
            return {}
        elif isinstance(packet.data, StopPacket):
            return self.__handle_stop(packet.data.city_name)

    @staticmethod
    def __handle_stop(_city_name: str) -> Dict[str, List[bytes]]:
        return {}

    def get_state(self) -> bytes:
        state = {
            "stations": self._stations,
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._stations = state["stations"]


def main():
    initialize_log(logging.INFO)
    aggregator = StationAggregator(int(REPLICA_ID), SIDE_TABLE_QUEUE_PREFIX, SIDE_TABLE_ROUTING_KEY)
    aggregator.start()


if __name__ == "__main__":
    main()
