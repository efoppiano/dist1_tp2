import logging
import os
import time
from typing import Iterator, List

from common.basic_client import BasicClient
from common.packets.dur_avg_out import DurAvgOut
from common.packets.station_dist_mean import StationDistMean
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.readers import TripInfo, StationInfo, WeatherInfo, WeatherReader, StationReader, TripReader
from common.utils import initialize_log

GATEWAY_PREFIX = os.environ["GATEWAY_PREFIX"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
REQ_ADDR = os.environ["REQ_ADDR"]
CITIES = os.environ["CITIES"].split(",")
DATA_FOLDER_PATH = os.environ["DATA_FOLDER_PATH"]


class Client(BasicClient):
    def __init__(self, config: dict):
        super().__init__(config)
        self._data_folder_path = config["data_folder_path"]

    def get_weather(self, city: str) -> Iterator[List[WeatherInfo]]:
        reader = WeatherReader(self._data_folder_path, city)
        yield from reader.next_data()

    def get_stations(self, city: str) -> Iterator[List[StationInfo]]:
        reader = StationReader(self._data_folder_path, city)
        yield from reader.next_data()

    def get_trips(self, city: str) -> Iterator[List[TripInfo]]:
        reader = TripReader(self._data_folder_path, city)
        yield from reader.next_data()

    def handle_dur_avg_out_packet(self, packet: DurAvgOut):
        logging.info(
            f"action: receive_dur_avg_packet | result: success | "
            f"city: {packet.city_name} | start_date: {packet.start_date} |  dur_avg_sec: {packet.dur_avg_sec}")

    def handle_trip_count_by_year_joined_packet(self, packet: TripsCountByYearJoined):
        logging.info(
            f"action: receive_trip_count_packet | result: success | city: {packet.city_name} |"
            f" start_station_name: {packet.start_station_name} |"
            f" trips (2016): {packet.trips_16} | trips (2017): {packet.trips_17}")

    def handle_station_dist_mean_packet(self, packet: StationDistMean):
        logging.info(
            f"action: receive_dist_mean_packet | result: success | city: {packet.city_name} |"
            f" end_station_name: {packet.end_station_name} | dist_mean (km): {packet.dist_mean}")


def main():
    initialize_log(logging.INFO)
    time.sleep(5)
    start_time = time.time()
    client = Client({
        "data_folder_path": DATA_FOLDER_PATH,
        "gateway_prefix": GATEWAY_PREFIX,
        "gateway_amount": GATEWAY_AMOUNT,
        "req_addr": REQ_ADDR,
        "cities": CITIES,
    })
    client.run()
    end_time = time.time()
    logging.info(f"action: client_run | result: success | duration: {end_time - start_time} sec")


if __name__ == "__main__":
    main()
