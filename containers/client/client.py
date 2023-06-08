import logging
import os
import time
from typing import Iterator, List

from common.basic_client import BasicClient
from common.packets.dur_avg_out import DurAvgOut
from common.packets.station_dist_mean import StationDistMean
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.readers import TripInfo, StationInfo, WeatherInfo, WeatherReader, StationReader, TripReader
from common.utils import initialize_log, json_serialize

CLIENT_ID = os.environ["CLIENT_ID"]
CITIES = os.environ["CITIES"].split(",")
DATA_FOLDER_PATH = os.environ["DATA_FOLDER_PATH"]


class Client(BasicClient):
    def __init__(self, config: dict):
        super().__init__(config)
        self._data_folder_path = config["data_folder_path"]
        self.results = {}

    def get_weather(self, city: str) -> Iterator[List[WeatherInfo]]:
        reader = WeatherReader(self._data_folder_path, city)
        yield from reader.next_data()

    def get_stations(self, city: str) -> Iterator[List[StationInfo]]:
        reader = StationReader(self._data_folder_path, city)
        yield from reader.next_data()

    def get_trips(self, city: str) -> Iterator[List[TripInfo]]:
        reader = TripReader(self._data_folder_path, city)
        yield from reader.next_data()

    def save_results(self, city, type, key, results):
        self.results.setdefault(city, {})
        self.results[city].setdefault(type, {})
        self.results[city][type][key] = results

    def dump_results(self):
        filename = f"{self._data_folder_path}/results/{self.client_id}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            data = json_serialize(self.results)
            f.write(data)

    def handle_dur_avg_out_packet(self, city_name: str, packet: DurAvgOut):
        self.save_results(
            city_name, "duration_average_prectot>=30mm", packet.start_date,
            {"avg": round(packet.dur_avg_sec), "count": packet.dur_avg_amount}
        )
        logging.info(
            f"action: receive_dur_avg_packet | result: success | "
            f"city: {city_name} | start_date: {packet.start_date} |  dur_avg_sec: {round(packet.dur_avg_sec, 2)} | amount: {packet.dur_avg_amount}")

    def handle_trip_count_by_year_joined_packet(self, city_name: str, packet: TripsCountByYearJoined):
        self.save_results(
            city_name, "trip_count_by_year", packet.start_station_name,
            {"2016": packet.trips_16, "2017": packet.trips_17}
        )
        logging.info(
            f"action: receive_trip_count_packet | result: success | city: {city_name} |"
            f" start_station_name: {packet.start_station_name} |"
            f" trips (2016): {packet.trips_16} | trips (2017): {packet.trips_17}")

    def handle_station_dist_mean_packet(self, city_name: str, packet: StationDistMean):
        self.save_results(
            city_name, "stations_mean_dist_>=6km", packet.end_station_name,
            {"mean_dist": round(packet.dist_mean)}
        )
        logging.info(
            f"action: receive_dist_mean_packet | result: success | city: {city_name} |"
            f" end_station_name: {packet.end_station_name} | dist_mean (km): {round(packet.dist_mean, 2)}")


def main():
    initialize_log(logging.INFO)
    time.sleep(5)
    start_time = time.time()
    client = Client({
        "data_folder_path": DATA_FOLDER_PATH,
        "client_id": CLIENT_ID,
        "cities": CITIES,
    })
    client.run()
    end_time = time.time()
    client.dump_results()
    logging.info(f"action: client_run | result: success | duration: {end_time - start_time} sec")


if __name__ == "__main__":
    main()
