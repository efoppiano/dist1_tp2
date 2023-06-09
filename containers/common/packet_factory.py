from typing import List

from common.packets.generic_packet import GenericPacket
from common.readers import WeatherInfo, StationInfo, TripInfo, ClientGatewayPacket, ClientEofPacket, ClientIdPacket

DIST_MEAN_REQUEST = b'dist_mean'
TRIP_COUNT_REQUEST = b'trip_count'
DUR_AVG_REQUEST = b'dur_avg'

MAX_PACKET_ID = None # 2**10 - 1
def increment_packet_id(func):
    def wrapper(*args, **kwargs):
        PacketFactory.packet_id += 1
        if MAX_PACKET_ID and PacketFactory.packet_id > MAX_PACKET_ID:
            PacketFactory.packet_id = 0
        return func(*args, **kwargs)
    return wrapper

class PacketFactory:
    client_id = None
    replica_id = None
    packet_id = 0

    @staticmethod
    def set_ids( client_id: str ):
        PacketFactory.client_id = client_id
        PacketFactory.replica_id = 0

    @staticmethod
    @increment_packet_id
    def build_id_request_packet(client_id: str) -> bytes:
        return GenericPacket(
            replica_id= None,
            client_id= None,
            city_name= None,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(ClientIdPacket(client_id)).encode()
        ).encode()

    @staticmethod
    @increment_packet_id
    def build_weather_packet(city_name: str, weather_info: List[WeatherInfo]) -> bytes:
        return GenericPacket(
            replica_id= PacketFactory.replica_id,
            client_id= PacketFactory.client_id,
            city_name= city_name,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(weather_info).encode()
        ).encode()

    @staticmethod
    @increment_packet_id
    def build_weather_eof(city: str) -> bytes:
        return GenericPacket(
            replica_id= PacketFactory.replica_id,
            client_id= PacketFactory.client_id,
            city_name= city,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(
                ClientEofPacket("weather", PacketFactory.client_id, city)
            ).encode()
        ).encode()

    @staticmethod
    @increment_packet_id
    def build_station_packet(city_name: str, station_info: List[StationInfo]) -> bytes:
        return GenericPacket(
            replica_id= PacketFactory.replica_id,
            client_id= PacketFactory.client_id,
            city_name= city_name,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(station_info).encode()
        ).encode()

    @staticmethod
    @increment_packet_id
    def build_station_eof(city: str) -> bytes:
        return GenericPacket(
            replica_id= None,
            client_id= PacketFactory.client_id,
            city_name= city,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(
                ClientEofPacket("station", PacketFactory.client_id, city)
            ).encode()
        ).encode()


    @staticmethod
    @increment_packet_id
    def build_trip_packet(city_name: str, trip_info: List[TripInfo]) -> bytes:
        return GenericPacket(
            replica_id= None,
            client_id= PacketFactory.client_id,
            city_name= city_name,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(trip_info).encode()
        ).encode()
    

    @staticmethod
    @increment_packet_id
    def build_trip_eof(city: str) -> bytes:
        return GenericPacket(
            replica_id= None,
            client_id= PacketFactory.client_id,
            city_name= city,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(
                ClientEofPacket("trip", PacketFactory.client_id, city)
            ).encode()
        ).encode()

