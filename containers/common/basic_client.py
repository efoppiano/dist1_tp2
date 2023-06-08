import logging
import os
import threading
from abc import ABC, abstractmethod
from typing import List, Iterator

from common.linker.linker import Linker
from common.packet_factory import PacketFactory
from common.packets.dur_avg_out import DurAvgOut
from common.packets.client_response_packets import GenericResponsePacket
from common.packets.station_dist_mean import StationDistMean
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.rabbit_middleware import Rabbit
from common.readers import WeatherInfo, StationInfo, TripInfo

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")

EOF_TYPES = ["dist_mean_eof","trip_count_eof","dur_avg_eof"]

class BasicClient(ABC):
    def __init__(self, config: dict):
        self.client_id = config["client_id"]
        self._all_cities = config["cities"]
        PacketFactory.init(self.client_id)

        self._eofs = {}

        self._rabbit = Rabbit(RABBIT_HOST)

        self.__set_up_signal_handler()

    def __set_up_signal_handler(self):
        # TODO: Implement graceful shutdown
        pass

    @staticmethod
    @abstractmethod
    def get_weather(city: str) -> Iterator[List[WeatherInfo]]:
        pass

    @staticmethod
    @abstractmethod
    def get_stations(city: str) -> Iterator[List[StationInfo]]:
        pass

    @staticmethod
    @abstractmethod
    def get_trips(city: str) -> Iterator[List[TripInfo]]:
        pass

    @abstractmethod
    def handle_dur_avg_out_packet(self, city_name: str, packet: DurAvgOut):
        pass

    @abstractmethod
    def handle_trip_count_by_year_joined_packet(self, city_name: str, packet: TripsCountByYearJoined):
        pass

    @abstractmethod
    def handle_station_dist_mean_packet(self, city_name: str, packet: StationDistMean):
        pass

    def __send_weather_data(self, queue: str, city: str):
        for weather_info_list in self.get_weather(city):
            packet = PacketFactory.build_weather_packet(city, weather_info_list)
            self._rabbit.produce(queue,packet)

        self._rabbit.produce(queue, PacketFactory.build_weather_eof(city))

    def __send_stations_data(self, queue: str, city: str):
        for station_info_list in self.get_stations(city):
            packet = PacketFactory.build_station_packet(city, station_info_list)
            self._rabbit.produce(queue, packet)

        self._rabbit.produce(queue, PacketFactory.build_station_eof(city))

    def __send_trips_data(self, queue: str, city: str):
        for trip_info_list in self.get_trips(city):
            self._rabbit.produce(queue, PacketFactory.build_trip_packet(city, trip_info_list))

        self._rabbit.produce(queue, PacketFactory.build_trip_eof(city))

    def __send_data_from_city(self, city: str):
        logging.info(f"action: client_send_data | result: in_progress | city: {city}")
        queue_name = Linker().get_output_queue(self, hashing_key=city)

        try:
            self.__send_weather_data(queue_name, city)
            self.__send_stations_data(queue_name, city)
            self.__send_trips_data(queue_name, city)
            logging.info(f"action: client_send_data | result: success | city: {city}")
        except Exception as e:
            logging.error(f"action: client_send_data | result: error | city: {city} | error: {e}")
            raise e
        finally:
            logging.info(f"action: client_send_data | result: finished | city: {city}")

    def __send_cities_data(self):
        for city in self._all_cities:
            self.__send_data_from_city(city)

    def __handle_dist_mean( self, city_name: str, data: List[bytes] ):
        for item in data:
          station_dist_mean = StationDistMean.decode(item)
          self.handle_station_dist_mean_packet(city_name, station_dist_mean)
    
    def __handle_dur_avg( self, city_name: str, data: List[bytes] ):
        for item in data:
          dur_avg_out = DurAvgOut.decode(item)
          self.handle_dur_avg_out_packet(city_name, dur_avg_out)
    
    def __handle_trip_count( self, city_name: str, data: List[bytes] ):
        for item in data:
          trips_count = TripsCountByYearJoined.decode(item)
          self.handle_trip_count_by_year_joined_packet(city_name, trips_count)
    
    def __handle_eof( self, type: str, city_name: str ):
        self._eofs.setdefault(type, set())
        self._eofs[type].add(city_name)

    def __all_eofs_received(self) -> bool:
        for type in EOF_TYPES:
          if len(self._eofs.get(type, [])) != len(self._all_cities):
            return False

        return True

    def __handle_message(self, message: bytes) -> bool:
        packet = GenericResponsePacket.decode(message)
        city_name = packet.city_name
        
        if packet.type == "dist_mean":
          self.__handle_dist_mean(city_name, packet.data)
        elif packet.type == "dur_avg":
          self.__handle_dur_avg(city_name, packet.data)
        elif packet.type == "trip_count":
          self.__handle_trip_count(city_name, packet.data)
        elif packet.type in EOF_TYPES:
          self.__handle_eof(packet.type, city_name)
        else:
          logging.warning(f"Unexpected message type: {packet.type}")

        if self.__all_eofs_received():
          self._rabbit.stop()

        return True

    def __get_responses(self):
        
        queue = f"results_{self.client_id}"
        self._rabbit.consume(queue, self.__handle_message)

        self._rabbit.start()

    def __run(self):
        self.__send_cities_data()
        self.__get_responses()

    def run(self):
        thread = threading.Thread(target=self.__run)
        thread.start()
        thread.join()
