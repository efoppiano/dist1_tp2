import logging
import os
import threading
from abc import ABC, abstractmethod
from typing import List, Iterator

from common.linker.linker import Linker
from common.packet_factory import PacketFactory, DIST_MEAN_REQUEST, DUR_AVG_REQUEST, TRIP_COUNT_REQUEST
from common.packets.dur_avg_out import DurAvgOut
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket
from common.packets.station_dist_mean import StationDistMean
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.rabbit_middleware import Rabbit
from common.readers import WeatherInfo, StationInfo, TripInfo

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")


class BasicClient(ABC):
    def __init__(self, config: dict):
        self._client_id = config["client_id"]
        self._all_cities = config["cities"]

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
    def handle_dur_avg_out_packet(self, packet: DurAvgOut):
        pass

    @abstractmethod
    def handle_trip_count_by_year_joined_packet(self, packet: TripsCountByYearJoined):
        pass

    @abstractmethod
    def handle_station_dist_mean_packet(self, packet: StationDistMean):
        pass

    def __send_weather_data(self, queue: str, city: str):
        for weather_info_list in self.get_weather(city):
            self._rabbit.produce(queue, PacketFactory.build_weather_packet(weather_info_list))

        self._rabbit.produce(queue, PacketFactory.build_weather_eof(city))

    def __send_stations_data(self, queue: str, city: str):
        for station_info_list in self.get_stations(city):
            self._rabbit.produce(queue, PacketFactory.build_station_packet(station_info_list))

        self._rabbit.produce(queue, PacketFactory.build_station_eof(city))

    def __send_trips_data(self, queue: str, city: str):
        for trip_info_list in self.get_trips(city):
            self._rabbit.produce(queue, PacketFactory.build_trip_packet(trip_info_list))

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

    def __get_dur_avg_response(self, socket):
        cities_ended = []
        while len(cities_ended) < len(self._all_cities):
            socket.send(DUR_AVG_REQUEST)
            data = socket.recv()
            message = GenericPacket.decode(data)
            if isinstance(message.data, bytes):
                dur_avg_out = DurAvgOut.decode(message.data)
                self.handle_dur_avg_out_packet(dur_avg_out)
            elif isinstance(message.data, list):
                for packet_bytes in message.data:
                    packet = DurAvgOut.decode(packet_bytes)
                    self.handle_dur_avg_out_packet(packet)
            elif isinstance(message.data, Eof):
                city = message.data.city_name
                cities_ended.append(city)
            else:
                raise ValueError(f"Unexpected message type: {type(message)}")

    def __get_trip_count_response(self, socket):
        cities_ended = []
        while len(cities_ended) < len(self._all_cities):
            socket.send(TRIP_COUNT_REQUEST)
            data = socket.recv()
            message = GenericPacket.decode(data)
            if isinstance(message.data, bytes):
                trips_count = TripsCountByYearJoined.decode(message.data)
                self.handle_trip_count_by_year_joined_packet(trips_count)
            elif isinstance(message.data, list):
                for packet_bytes in message.data:
                    trips_count = TripsCountByYearJoined.decode(packet_bytes)
                    self.handle_trip_count_by_year_joined_packet(trips_count)
            elif isinstance(message.data, Eof):
                city = message.data.city_name
                cities_ended.append(city)

    def __get_dist_mean_response(self, socket):
        cities_ended = []
        while len(cities_ended) < len(self._all_cities):
            socket.send(DIST_MEAN_REQUEST)
            data = socket.recv()
            message = GenericPacket.decode(data)
            if isinstance(message.data, bytes):
                station_dist_mean = StationDistMean.decode(message.data)
                self.handle_station_dist_mean_packet(station_dist_mean)
            elif isinstance(message.data, list):
                for packet_bytes in message.data:
                    station_dist_mean = StationDistMean.decode(packet_bytes)
                    self.handle_station_dist_mean_packet(station_dist_mean)
            elif isinstance(message.data, Eof):
                city = message.data.city_name
                cities_ended.append(city)
            else:
                raise ValueError(f"Unexpected message type: {type(message)}")

    def __handle_message(self, message: bytes) -> bool:
        packet = GenericPacket.decode(message)
        logging.info(packet)

        return True

    def __get_responses(self):
        
        self._rabbit.consume("results", self.__handle_message)
        self._rabbit.start()

    def __run(self):
        self.__send_cities_data()
        self.__get_responses()

    def run(self):
        thread = threading.Thread(target=self.__run)
        thread.start()
        thread.join()
