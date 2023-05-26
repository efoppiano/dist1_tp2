import logging
import queue
import signal
import threading
from typing import Union, List

import zmq

from common.linker.linker import Linker
from common.packet_factory import PacketFactory
from common.packets.aggregator_packets import ChunkOrStop, StopPacket
from common.packets.eof_with_id import EofWithId
from common.packets.generic_packet import GenericPacket
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.rabbit_middleware import Rabbit
from common.readers import WeatherInfo, StationInfo, TripInfo
from common.utils import initialize_log, datetime_str_to_date_str


class Gateway:
    def __init__(self, zmq_addr: str):
        self._zmq_addr = zmq_addr
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

    @staticmethod
    def __build_weather_side_table_info_chunk(weather_info_list: List[WeatherInfo]) -> List[bytes]:
        chunk = []
        for weather_info in weather_info_list:
            data_packet = WeatherSideTableInfo(weather_info.city_name, weather_info.date, weather_info.prectot).encode()
            chunk.append(data_packet)
        return chunk

    def __handle_weather_packet(self, weather_info_list_or_city_eof: Union[List[WeatherInfo], str]):
        if isinstance(weather_info_list_or_city_eof, str):
            city_name = weather_info_list_or_city_eof
            self.__schedule_message_to_publish(f"weather", ChunkOrStop(StopPacket(city_name)).encode())
        else:
            chunk = self.__build_weather_side_table_info_chunk(weather_info_list_or_city_eof)
            self.__schedule_message_to_publish(f"weather", ChunkOrStop(chunk).encode())

    @staticmethod
    def __build_station_side_table_info_chunk(station_info_list: List[StationInfo]) -> List[bytes]:
        chunk = []
        for station_info in station_info_list:
            data_packet = StationSideTableInfo(station_info.city_name, station_info.code, station_info.yearid,
                                               station_info.name, station_info.latitude,
                                               station_info.longitude).encode()
            chunk.append(data_packet)
        return chunk

    def __handle_station_packet(self, station_info_list_or_city_eof: Union[List[StationInfo], str]):
        if isinstance(station_info_list_or_city_eof, str):
            city_name = station_info_list_or_city_eof
            self.__schedule_message_to_publish(f"station", ChunkOrStop(StopPacket(city_name)).encode())
        else:
            station_info_list = station_info_list_or_city_eof

            chunk = self.__build_station_side_table_info_chunk(station_info_list)

            data_packet = ChunkOrStop(chunk).encode()

            self.__schedule_message_to_publish(f"station", data_packet)

    def __handle_trip_packet(self, trip_info_list_or_city_eof: Union[List[TripInfo], str]):
        if isinstance(trip_info_list_or_city_eof, str):
            city_name = trip_info_list_or_city_eof
            queue_name = Linker().get_eof_in_queue(self)
            self.__schedule_message_to_produce(queue_name, EofWithId(city_name, 1).encode())
        else:
            trip_info_list = trip_info_list_or_city_eof
            first_trip_info = trip_info_list[0]
            first_start_date = datetime_str_to_date_str(first_trip_info.start_datetime)
            message = GenericPacket(1, [trip_info.encode() for trip_info in trip_info_list])

            queue_name = Linker().get_output_queue(self, hashing_key=first_start_date)
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
    gateway = Gateway("tcp://0.0.0.0:5555")
    gateway.start()


if __name__ == "__main__":
    main()
