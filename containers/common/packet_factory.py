import logging
from typing import List

from common.readers import WeatherInfo, StationInfo, TripInfo, ClientGatewayPacket, ClientEofPacket

DIST_MEAN_REQUEST = b'dist_mean'
TRIP_COUNT_REQUEST = b'trip_count'
DUR_AVG_REQUEST = b'dur_avg'


class PacketFactory:
    @staticmethod
    def build_weather_packet(weather_info: List[WeatherInfo]) -> bytes:
        return ClientGatewayPacket(
            weather_info
        ).encode()

    @staticmethod
    def build_weather_eof(city: str) -> bytes:
        return ClientGatewayPacket(
            ClientEofPacket("weather", city)
        ).encode()

    @staticmethod
    def build_station_packet(station_info: List[StationInfo]) -> bytes:
        return ClientGatewayPacket(
            station_info
        ).encode()

    @staticmethod
    def build_station_eof(city: str) -> bytes:
        return ClientGatewayPacket(
            ClientEofPacket("station", city)
        ).encode()

    @staticmethod
    def build_trip_packet(trip_info: List[TripInfo]) -> bytes:
        return ClientGatewayPacket(
            trip_info
        ).encode()

    @staticmethod
    def build_trip_eof(city: str) -> bytes:
        return ClientGatewayPacket(
            ClientEofPacket("trip", city)
        ).encode()

    @staticmethod
    def handle_packet(data: bytes, weather_callback, station_callback, trip_callback):
        try:
            decoded: ClientGatewayPacket = ClientGatewayPacket.decode(data)
        except Exception as e:
            logging.error(f"action: decode_client_gateway_packet | result: failure | error: {e}")
            raise e

        if isinstance(decoded.packet, ClientEofPacket):
            packet_type = decoded.packet.file_type
            city_name = decoded.packet.city_name
            if packet_type == "weather":
                weather_callback(city_name)
            elif packet_type == "station":
                station_callback(city_name)
            elif packet_type == "trip":
                trip_callback(city_name)
            else:
                raise ValueError(f"Unknown packet type: {packet_type}")
        elif isinstance(decoded.packet, list):
            element_type = type(decoded.packet[0])
            if element_type == WeatherInfo:
                weather_callback(decoded.packet)
            elif element_type == StationInfo:
                station_callback(decoded.packet)
            elif element_type == TripInfo:
                trip_callback(decoded.packet)
            else:
                raise ValueError(f"Unknown packet type: {element_type}")
        else:
            raise ValueError(f"Unknown packet type: {type(decoded.packet)}")
