import logging
from typing import List

from common.packets.generic_packet import GenericPacket
from common.readers import WeatherInfo, StationInfo, TripInfo, ClientGatewayPacket, ClientEofPacket

DIST_MEAN_REQUEST = b'dist_mean'
TRIP_COUNT_REQUEST = b'trip_count'
DUR_AVG_REQUEST = b'dur_avg'


class PacketFactory:
    @staticmethod
    def build_weather_packet(weather_info: List[WeatherInfo]) -> bytes:
        return GenericPacket(1, ClientGatewayPacket(
            weather_info
        ).encode()).encode()

    @staticmethod
    def build_weather_eof(city: str) -> bytes:
        return GenericPacket(1, ClientGatewayPacket(
            ClientEofPacket("weather", city)
        ).encode()).encode()

    @staticmethod
    def build_station_packet(station_info: List[StationInfo]) -> bytes:
        return GenericPacket(1, ClientGatewayPacket(
            station_info
        ).encode()).encode()

    @staticmethod
    def build_station_eof(city: str) -> bytes:
        return GenericPacket(1, ClientGatewayPacket(
            ClientEofPacket("station", city)
        ).encode()).encode()

    @staticmethod
    def build_trip_packet(trip_info: List[TripInfo]) -> bytes:
        return GenericPacket(1, ClientGatewayPacket(
            trip_info
        ).encode()).encode()

    @staticmethod
    def build_trip_eof(city: str) -> bytes:
        return GenericPacket(1, ClientGatewayPacket(
            ClientEofPacket("trip", city)
        ).encode()).encode()

    @staticmethod
    def handle_packet(data: bytes, weather_callback, station_callback, trip_callback):
        try:
            decoded: ClientGatewayPacket = ClientGatewayPacket.decode(data)
        except Exception as e:
            logging.error(f"action: decode_client_gateway_packet | result: failure | error: {e}")
            raise e

        if isinstance(decoded.data, ClientEofPacket):
            packet_type = decoded.data.file_type
            city_name = decoded.data.city_name
            if packet_type == "weather":
                weather_callback(city_name)
            elif packet_type == "station":
                station_callback(city_name)
            elif packet_type == "trip":
                trip_callback(city_name)
            else:
                raise ValueError(f"Unknown packet type: {packet_type}")
        elif isinstance(decoded.data, list):
            element_type = type(decoded.data[0])
            if element_type == WeatherInfo:
                weather_callback(decoded.data)
            elif element_type == StationInfo:
                station_callback(decoded.data)
            elif element_type == TripInfo:
                trip_callback(decoded.data)
            else:
                raise ValueError(f"Unknown packet type: {element_type}")
        else:
            raise ValueError(f"Unknown packet type: {type(decoded.data)}")
