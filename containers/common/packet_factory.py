from typing import List

from common.packets.generic_packet import GenericPacket
from common.readers import WeatherInfo, StationInfo, TripInfo, ClientGatewayPacket, ClientEofPacket, ClientIdPacket

DIST_MEAN_REQUEST = b'dist_mean'
TRIP_COUNT_REQUEST = b'trip_count'
DUR_AVG_REQUEST = b'dur_avg'



class PacketFactory:
    client_id = None
    replica_id = None
    packet_id = None

    @staticmethod
    def set_ids( client_id: str ):
        PacketFactory.client_id = client_id
        PacketFactory.replica_id = 0

    @staticmethod
    def build_id_request_packet(client_id: str) -> bytes:
        return GenericPacket(
            replica_id= None,
            client_id= None,
            city_name= None,
            packet_id= None,
            data=ClientGatewayPacket(ClientIdPacket(client_id)).encode()
        ).encode()

    @staticmethod
    def build_weather_packet(city_name: str, weather_info: List[WeatherInfo]) -> bytes:
        return GenericPacket(
            replica_id= PacketFactory.replica_id,
            client_id= PacketFactory.client_id,
            city_name= city_name,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(weather_info).encode()
        ).encode()

    @staticmethod
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
    def build_station_packet(city_name: str, station_info: List[StationInfo]) -> bytes:
        return GenericPacket(
            replica_id= PacketFactory.replica_id,
            client_id= PacketFactory.client_id,
            city_name= city_name,
            packet_id= PacketFactory.packet_id,
            data=ClientGatewayPacket(station_info).encode()
        ).encode()

    @staticmethod
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
    def build_trip_packet(city_name: str, trip_info: List[TripInfo]) -> bytes:
        return GenericPacket(
            replica_id= None,
            client_id= PacketFactory.client_id,
            city_name= city_name,
            packet_id= None,
            data=ClientGatewayPacket(trip_info).encode()
        ).encode()
    

    @staticmethod
    def build_trip_eof(city: str) -> bytes:
        return GenericPacket(
            replica_id= None,
            client_id= PacketFactory.client_id,
            city_name= city,
            packet_id= None,
            data=ClientGatewayPacket(
                ClientEofPacket("trip", PacketFactory.client_id, city)
            ).encode()
        ).encode()

