#!/usr/bin/env python3
import logging
import os
from typing import Dict, List, Union, Tuple

from common.basic_aggregator import BasicAggregator
from common.linker.linker import Linker
from common.packets.distance_calc_in import DistanceCalcIn
from common.packets.eof import Eof
from common.packets.gateway_out import GatewayOut
from common.packets.prec_filter_in import PrecFilterIn
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.year_filter_in import YearFilterIn
from common.utils import initialize_log

SIDE_TABLE_QUEUE_NAME = os.environ["SIDE_TABLE_QUEUE_NAME"]
REPLICA_ID = os.environ["REPLICA_ID"]


class StationAggregator(BasicAggregator):
    def __init__(self, replica_id: int, side_table_queue_name: str):
        super().__init__(replica_id, side_table_queue_name)

        self._stations = {}
        self._unanswered_packets = {}
        self._stopped_cities = set()

    def handle_eof(self, message: Eof) -> Dict[str, List[bytes]]:
        # TODO: Delay EOF if there are unanswered packets for this client
        logging.info(f"action: handle_eof | result: in_progress | city_name: {message.city_name}")

        eof_year_filter_queue = Linker().get_eof_in_queue(self, "YearFilter")
        eof_prec_filter_queue = Linker().get_eof_in_queue(self, "PrecFilter")
        eof_distance_calc_queue = Linker().get_eof_in_queue(self, "DistanceCalculator")

        return {
            eof_year_filter_queue: [Eof(message.city_name).encode()],
            eof_prec_filter_queue: [Eof(message.city_name).encode()],
            eof_distance_calc_queue: [Eof(message.city_name).encode()],
        }

    def handle_side_table_message(self, message: bytes):
        packet = StationSideTableInfo.decode(message)
        city_name, station_code, yearid = packet.city_name, packet.station_code, packet.yearid

        self._stations.setdefault(city_name, {})

        self._stations[city_name][(station_code, yearid)] = {
            "station_name": packet.station_name,
            "latitude": packet.latitude,
            "longitude": packet.longitude,
        }

    def __search_station(self, city_name: str, station_code: int, yearid: int) -> Union[dict, None]:
        try:
            return self._stations[city_name][(station_code, yearid)]
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
            packet.city_name, packet.start_date, packet.duration_sec, packet.prectot
        )

        year_filter_in_packet = YearFilterIn(
            packet.city_name, start_station_info["station_name"], packet.yearid
        )

        if start_station_info["latitude"]:
            distance_calc_in_packet = DistanceCalcIn(
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

    def handle_message(self, message: bytes) -> Dict[str, List[bytes]]:
        packet = GatewayOut.decode(message)

        stations = self.__search_stations(packet)
        if not stations:
            if packet.city_name not in self._stopped_cities:
                self._unanswered_packets.setdefault(packet.city_name, [])
                self._unanswered_packets[packet.city_name].append(packet)
            else:
                logging.warning(f"Could not find stations for packet: {packet}")
            return {}

        prec_filter_queue = Linker().get_output_queue(self, "PrecFilter", str(packet.start_station_code))
        year_filter_queue = Linker().get_output_queue(self, "YearFilter", str(packet.start_station_code))
        distance_calc_queue = Linker().get_output_queue(self, "DistanceCalculator", str(packet.start_station_code))

        output_packets_lists = self.__build_packet_lists(packet, stations)

        return {
            prec_filter_queue: output_packets_lists[0],
            year_filter_queue: output_packets_lists[1],
            distance_calc_queue: output_packets_lists[2],
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
    aggregator = StationAggregator(int(REPLICA_ID), SIDE_TABLE_QUEUE_NAME)
    aggregator.start()


if __name__ == "__main__":
    main()
