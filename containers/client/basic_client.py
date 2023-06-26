import logging
import os
import signal
import random
import datetime
import time
from abc import ABC, abstractmethod
from typing import List, Iterator

from common.components.invoker import Invoker
from common.packets.client_control_packet import ClientControlPacket, RateLimitChangeRequest
from packet_factory import PacketFactory
from common.packets.dur_avg_out import DurAvgOut
from common.packets.client_response_packets import GenericResponsePacket
from common.packets.eof import Eof
from common.packets.station_dist_mean import StationDistMean
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined
from common.middleware.rabbit_middleware import Rabbit
from common.components.readers import WeatherInfo, StationInfo, TripInfo, ClientIdResponsePacket
from common.router import Router
from common.utils import log_msg, success, bold, append_signal

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
ID_REQ_QUEUE = os.environ["ID_REQ_QUEUE"]
CLIENT_ID = os.environ["CLIENT_ID"]

GATEWAY = os.environ["GATEWAY"]
GATEWAY_AMOUNT = int(os.environ["GATEWAY_AMOUNT"])
CONTROL_QUEUE_PREFIX = os.environ.get("CONTROL_QUEUE_PREFIX", "control_")
RESULTS_QUEUE_PREFIX = os.environ.get("RESULTS_QUEUE_PREFIX", "results_")
EOF_TYPES = ["dist_mean", "trip_count", "dur_avg"]

INITIAL_SEND_RATE = int(os.environ.get("INITIAL_SEND_RATE", 10))
INVOKER_WAIT_TIME = int(os.environ.get("INVOKER_WAIT_TIME", 3))
CONTROL_TIMEOUT = float(os.environ.get("CONTROL_TIMEOUT", 0.1))
LOG_RATE_CHANCE = 0.3


class BasicClient(ABC):
    def __init__(self, config: dict):
        timestamp = datetime.datetime.now().strftime("%Y-%m%d-%H%M")
        self.client_id = f'{config["client_id"]}-{timestamp}'
        self.session_id = None  # Assigned by the server
        self.finished = False
        self.canceled = False
        self.router = Router(GATEWAY, GATEWAY_AMOUNT)

        self._all_cities = config["cities"]
        self._eofs = {}
        self._send_rate = INITIAL_SEND_RATE
        self._invoker = Invoker(INVOKER_WAIT_TIME, self.__check_control_queue)

        self._rabbit = Rabbit(RABBIT_HOST)
        self.__set_up_signal_handler()

        PacketFactory.set_ids(self.__request_session_id())

    def handle_signal(self, signum, frame):
        self.canceled = True

    def __set_up_signal_handler(self):
        append_signal(signal.SIGINT, self.handle_signal)
        append_signal(signal.SIGTERM, self.handle_signal)

    def __request_session_id(self) -> str:
        queue_name = self.router.route(self.client_id)
        packet = PacketFactory.build_id_request_packet()
        self._rabbit.produce(queue_name, packet)

        def on_client_id_packet(client_id_packet: bytes):
            response = ClientIdResponsePacket.decode(client_id_packet)
            session_id = response.client_id

            self.session_id = session_id
            success(f"Assigned Session Id: {session_id}")
            return True

        response_queue = ID_REQ_QUEUE
        self._rabbit.consume_one(response_queue, on_client_id_packet)
        return self.session_id

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
            if self.canceled:
                return
            packet = PacketFactory.build_weather_packet(city, weather_info_list)
            self._rabbit.produce(queue, packet)
            self._invoker.check()
            time.sleep(1 / self._send_rate)

    def __send_stations_data(self, queue: str, city: str):
        for station_info_list in self.get_stations(city):
            if self.canceled:
                return
            packet = PacketFactory.build_station_packet(city, station_info_list)
            self._rabbit.produce(queue, packet)
            self._invoker.check()
            time.sleep(1 / self._send_rate)

    def __send_trips_data(self, queue: str, city: str, last: bool = False):
        for trip_info_list in self.get_trips(city):
            if self.canceled:
                return
            self._rabbit.produce(queue, PacketFactory.build_trip_packet(city, trip_info_list))
            self._invoker.check()
            time.sleep(1 / self._send_rate)

        self.finished = last
        self._rabbit.produce(queue, PacketFactory.build_trip_eof(city))

    def __send_data_from_city(self, city: str, last: bool = False):
        logging.info(f"action: client_send_data | result: in_progress | city: {city}")
        queue_name = self.router.route(hashing_key=self.client_id)

        if self.canceled:
            return

        try:
            self.__send_weather_data(queue_name, city)
            self.__send_stations_data(queue_name, city)
            self.__send_trips_data(queue_name, city, last)
            logging.info(f"sent_data | city: {city}")
        except Exception as e:
            logging.error(f"send_data | city: {city} | error: {e}")
            if self.canceled:
                return
            raise e

    def __send_cities_data(self):
        for city in self._all_cities:
            last = city == self._all_cities[-1]
            self.__send_data_from_city(city, last)

    def __handle_dist_mean(self, city_name: str, data: List[bytes]):
        for item in data:
            station_dist_mean = StationDistMean.decode(item)
            self.handle_station_dist_mean_packet(city_name, station_dist_mean)

    def __handle_dur_avg(self, city_name: str, data: List[bytes]):
        for item in data:
            dur_avg_out = DurAvgOut.decode(item)
            self.handle_dur_avg_out_packet(city_name, dur_avg_out)

    def __handle_trip_count(self, city_name: str, data: List[bytes]):
        for item in data:
            trips_count = TripsCountByYearJoined.decode(item)
            self.handle_trip_count_by_year_joined_packet(city_name, trips_count)

    def __handle_eof(self, eof_type: str, city_name: str):
        self._eofs.setdefault(eof_type, set())
        self._eofs[eof_type].add(city_name)
        logging.info(f"Received all {bold(eof_type)} results for {bold(city_name)}")

    def __all_eofs_received(self) -> bool:
        for eof_type in EOF_TYPES:
            if len(self._eofs.get(eof_type, [])) != len(self._all_cities):
                return False

        return True

    def __handle_message(self, message: bytes) -> bool:

        if self.canceled:
            self._rabbit.safe_close()

        packet = GenericResponsePacket.decode(message)
        city_name = packet.city_name

        if isinstance(packet.data, Eof):
            self.__handle_eof(packet.type, city_name)
        elif packet.type == "dist_mean":
            self.__handle_dist_mean(city_name, packet.data)
        elif packet.type == "dur_avg":
            self.__handle_dur_avg(city_name, packet.data)
        elif packet.type == "trip_count":
            self.__handle_trip_count(city_name, packet.data)
        else:
            logging.warning(f"Unexpected message type: {packet.type}")

        if self.__all_eofs_received():
            self._rabbit.stop()

        return True

    def __check_control_queue(self):
        queue = CONTROL_QUEUE_PREFIX + str(self.session_id)
        self._rabbit.consume_one(queue, self.__handle_control_message, timeout=CONTROL_TIMEOUT, create=False)

    def __handle_control_message(self, message: bytes) -> bool:
        client_control_packet = ClientControlPacket.decode(message)
        if isinstance(client_control_packet.data, RateLimitChangeRequest):
            self._send_rate = client_control_packet.data.new_rate
            if random.random() < LOG_RATE_CHANCE:
                log_msg(f"Rate limit changed to {self._send_rate}")
        elif client_control_packet.data == "SessionExpired":
            if not self.finished:
                self._rabbit.close()
                logging.critical("Session expired " + str(self.session_id))
                raise ConnectionAbortedError("SessionExpired")
        else:
            raise NotImplementedError(f"Unknown control packet type: {type(client_control_packet)}")

        return True

    def __get_responses(self):

        if self.canceled:
            return

        results_queue = RESULTS_QUEUE_PREFIX + str(self.session_id)
        self._rabbit.consume(results_queue, self.__handle_message, create=False)

        # control_queue not needed, since we are no longer sending packets

        self._rabbit.start()

    def run(self):
        self.__send_cities_data()
        self.__get_responses()

    def close(self):
        self.canceled = True
        self._rabbit.close()

