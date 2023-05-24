import logging
import os
import queue
import signal
import threading
from typing import Union, List

import zmq

from common.packet_factory import PacketFactory
from common.packets.aggregator_packets import ChunkOrStop, StopPacket
from common.packets.basic_station_side_table_info import BasicStationSideTableInfo
from common.packets.eof import Eof
from common.packets.full_station_side_table_info import FullStationSideTableInfo
from common.packets.generic_packet import GenericPacket
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.rabbit_middleware import Rabbit
from common.readers import WeatherInfo, StationInfo, TripInfo
from common.utils import initialize_log, build_prefixed_hashed_queue_name, datetime_str_to_date_str

MONTREAL_OUTPUT_AMOUNT = int(os.environ["MONTREAL_OUTPUT_AMOUNT"])
TORONTO_OUTPUT_AMOUNT = int(os.environ["TORONTO_OUTPUT_AMOUNT"])
WASHINGTON_OUTPUT_AMOUNT = int(os.environ["WASHINGTON_OUTPUT_AMOUNT"])


class Gateway:
    def __init__(self, zmq_addr: str, output_amounts: dict):
        self._zmq_addr = zmq_addr
        self._output_amounts = output_amounts
        self._context = zmq.Context()

        self._mpmc_queue = queue.Queue()
        rabbit = Rabbit("rabbitmq")
        self._listener_thread = threading.Thread(target=self.__listen_queue_and_send_to_rabbit, args=(rabbit,))
        self._listener_thread.start()

        self.__set_up_signal_handler()

    def __listen_queue_and_send_to_rabbit(self, rabbit):

        while True:
            message = self._mpmc_queue.get()
            if message["type"] == "produce":
                rabbit.produce(message["queue"], message["message"])
            else:
                rabbit.publish(message["queue"], message["message"])

    def __schedule_message_to_produce(self, queue: str, message: bytes):
        self._mpmc_queue.put({"type": "produce", "queue": queue, "message": message})

    def __schedule_message_to_publish(self, queue: str, message: bytes):
        self._mpmc_queue.put({"type": "publish", "queue": queue, "message": message})

    def __set_up_signal_handler(self):
        def signal_handler(sig, frame):
            logging.info("action: shutdown_gateway | result: in_progress")
            self._context.term()
            logging.info("action: shutdown_gateway | result: success")
            if self.sig_hand_prev is not None:
                self.sig_hand_prev(sig, frame)

        self.sig_hand_prev = signal.signal(signal.SIGTERM, signal_handler)

    def __build_weather_side_table_info_chunk(self, weather_info_list: List[WeatherInfo]) -> List[bytes]:
        chunk = []
        for weather_info in weather_info_list:
            data_packet = WeatherSideTableInfo(weather_info.date, weather_info.prectot).encode()
            chunk.append(data_packet)
        return chunk

    def __handle_weather_packet(self, weather_info_list_or_city_eof: Union[List[WeatherInfo], str]):
        if isinstance(weather_info_list_or_city_eof, str):
            city_name = weather_info_list_or_city_eof
            self.__schedule_message_to_publish(f"{city_name}_weather", ChunkOrStop(StopPacket()).encode())
        else:
            weather_info_list = weather_info_list_or_city_eof
            first_weather_info = weather_info_list[0]
            chunk = self.__build_weather_side_table_info_chunk(weather_info_list_or_city_eof)
            self.__schedule_message_to_publish(f"{first_weather_info.city_name}_weather", ChunkOrStop(chunk).encode())

    def __build_basic_station_side_table_info_chunk(self, station_info_list: List[StationInfo]) -> List[bytes]:
        chunk = []
        for station_info in station_info_list:
            data_packet = BasicStationSideTableInfo(station_info.code, station_info.yearid,
                                                    station_info.name).encode()
            chunk.append(data_packet)
        return chunk

    def __build_full_station_side_table_info_chunk(self, station_info_list: List[StationInfo]) -> List[bytes]:
        chunk = []
        for station_info in station_info_list:
            data_packet = FullStationSideTableInfo(station_info.code, station_info.yearid,
                                                   station_info.name, station_info.latitude,
                                                   station_info.longitude).encode()
            chunk.append(data_packet)
        return chunk

    def __handle_station_packet(self, station_info_list_or_city_eof: Union[List[StationInfo], str]):
        if isinstance(station_info_list_or_city_eof, str):
            city_name = station_info_list_or_city_eof
            self.__schedule_message_to_publish(f"{city_name}_station", ChunkOrStop(StopPacket()).encode())
        else:
            station_info_list = station_info_list_or_city_eof
            first_station_info = station_info_list[0]
            if first_station_info.latitude is None or first_station_info.longitude is None:
                chunk = self.__build_basic_station_side_table_info_chunk(station_info_list)
            else:
                chunk = self.__build_full_station_side_table_info_chunk(station_info_list)

            data_packet = ChunkOrStop(chunk).encode()

            self.__schedule_message_to_publish(f"{first_station_info.city_name}_station", data_packet)

    def __handle_trip_packet(self, trip_info_list_or_city_eof: Union[List[TripInfo], str]):
        if isinstance(trip_info_list_or_city_eof, str):
            city_name = trip_info_list_or_city_eof
            self.__schedule_message_to_produce(f"{city_name}_gateway_in_eof_in", Eof(None).encode())
        else:
            trip_info_list = trip_info_list_or_city_eof
            first_trip_info = trip_info_list[0]
            city_name = first_trip_info.city_name
            first_start_date = datetime_str_to_date_str(first_trip_info.start_datetime)
            message = GenericPacket([trip_info.encode() for trip_info in trip_info_list])

            queue_name = build_prefixed_hashed_queue_name(city_name, "gateway_in", first_start_date,
                                                          self._output_amounts[city_name])
            self.__schedule_message_to_produce(queue_name, message.encode())

    def __start(self):
        socket = self._context.socket(zmq.PULL)
        socket.setsockopt(zmq.LINGER, 0)
        socket.bind(self._zmq_addr)
        try:
            while True:
                message = bytes(socket.recv())
                PacketFactory.handle_packet(message,
                                            self.__handle_weather_packet,
                                            self.__handle_station_packet,
                                            self.__handle_trip_packet)
        except Exception as e:
            logging.info(f"action: gateway_start | status: interrupted | error: {e}")
            raise e
        finally:
            socket.close()

    def start(self):
        thread = threading.Thread(target=self.__start)
        thread.start()
        thread.join()


def main():
    initialize_log(logging.INFO)
    gateway = Gateway("tcp://0.0.0.0:5555", {
        "montreal": MONTREAL_OUTPUT_AMOUNT,
        "toronto": TORONTO_OUTPUT_AMOUNT,
        "washington": WASHINGTON_OUTPUT_AMOUNT
    })
    gateway.start()


if __name__ == "__main__":
    main()
