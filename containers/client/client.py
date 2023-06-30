import logging
import os
import json
import time
from typing import List, Iterator

from basic_client import BasicClient
from compare_results import compare_results
from common.packets.dur_avg_out import DurAvgOut
from common.packets.station_dist_mean import StationDistMean
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.components.readers import TripInfo, StationInfo, WeatherInfo, WeatherReader, StationReader, TripReader
from common.utils import initialize_log, json_serialize, log_duplicate, success, log_msg

CLIENT_ID = os.environ["CLIENT_ID"]
CITIES = os.environ["CITIES"].split(",")
DATA_FOLDER_PATH = os.environ["DATA_FOLDER_PATH"]
BASELINE= os.environ.get("BASELINE","baseline.json")


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

        if key in self.results[city][type]:
            log_duplicate(f"city: {city} | type: {type} | key: {key}")

        self.results[city][type][key] = results

    def dump_results(self):
        filename = f"{self._data_folder_path}/results/{self.client_id}.json"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            data = json_serialize(self.results)
            f.write(data)
        return self.results 

    def handle_dur_avg_out_packet(self, city_name: str, packet: DurAvgOut):
        self.save_results(
            city_name, "duration_average_prectot>=30mm", packet.start_date,
            {"avg": round(packet.dur_avg_sec, 2), "count": packet.dur_avg_amount}
        )
        log_msg(
            f"receive dur_avg_packet |"
            f" city: {city_name} | start_date: {packet.start_date} |  dur_avg_sec: {round(packet.dur_avg_sec, 2)} | amount: {packet.dur_avg_amount}")

    def handle_trip_count_by_year_joined_packet(self, city_name: str, packet: TripsCountByYearJoined):
        self.save_results(
            city_name, "trip_count_by_year", packet.start_station_name,
            {"2016": packet.trips_16, "2017": packet.trips_17}
        )
        log_msg(
            f"receive trip_count_packet | city: {city_name} |"
            f" start_station_name: {packet.start_station_name} |"
            f" trips (2016): {packet.trips_16} | trips (2017): {packet.trips_17}")

    def handle_station_dist_mean_packet(self, city_name: str, packet: StationDistMean):
        self.save_results(
            city_name, "stations_mean_dist_>=6km", packet.end_station_name,
            {"mean_dist": round(packet.dist_mean, 2), "count": packet.dist_mean_amount}
        )
        log_msg(
            f"receive dist_mean_packet | city: {city_name} |"
            f" end_station_name: {packet.end_station_name} | dist_mean (km): {round(packet.dist_mean, 2)}")


def main():
    initialize_log(15)
    client = Client({
        "data_folder_path": DATA_FOLDER_PATH,
        "client_id": CLIENT_ID,
        "cities": CITIES,
    })
    time.sleep(5)
    
    logging.info(f"action: client_run | result: start")
    start_time = time.time()
    client.run()
    client.close()
    end_time = time.time()
    
    results = client.dump_results()
    success(f"action: client_run | duration: {end_time - start_time} sec | output_file: {client.client_id}")
    
    with open(f"{DATA_FOLDER_PATH}/results/{BASELINE}", "r") as f:
      baseline = json.load(f)
      same = compare_results(baseline, results)
      
    if not same:
      raise Exception("")


if __name__ == "__main__":
    main()
