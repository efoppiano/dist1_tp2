from typing import List
import logging

from common.packets.client_packet import ClientDataPacket, ClientPacket
from common.packets.eof import Eof
from common.components.readers import WeatherInfo, StationInfo, TripInfo, ClientGatewayPacket
from common.utils import min_hash, trace

DIST_MEAN_REQUEST = b'dist_mean'
TRIP_COUNT_REQUEST = b'trip_count'
DUR_AVG_REQUEST = b'dur_avg'

MAX_SEQ_NUMBER = 2 ** 10 - 1


class PacketFactory:
    client_id = None
    seq_number = 0

    @staticmethod
    def set_ids(client_id: str):
        PacketFactory.client_id = client_id
        PacketFactory.replica_id = 0

    @staticmethod
    def next_seq_number():
        PacketFactory.seq_number += 1
        if MAX_SEQ_NUMBER and PacketFactory.seq_number > MAX_SEQ_NUMBER:
            PacketFactory.seq_number = 0
        return PacketFactory.seq_number

    @staticmethod
    def build_id_request_packet() -> bytes:
        return ClientPacket(
            data="IdRequest"
        ).encode()

    @staticmethod
    def build_weather_packet(city_name: str, weather_info: List[WeatherInfo]) -> bytes:
        data_packet = ClientDataPacket(
            client_id=PacketFactory.client_id,
            city_name=city_name,
            seq_number=PacketFactory.next_seq_number(),
            data=[ClientGatewayPacket(weather_info).encode()]
        )
        return ClientPacket(data=data_packet).encode()

    @staticmethod
    def build_station_packet(city_name: str, station_info: List[StationInfo]) -> bytes:
        data_packet = ClientDataPacket(
            client_id=PacketFactory.client_id,
            city_name=city_name,
            seq_number=PacketFactory.next_seq_number(),
            data=[ClientGatewayPacket(station_info).encode()]
        )
        return ClientPacket(data=data_packet).encode()

    @staticmethod
    def build_trip_packet(city_name: str, trip_info: List[TripInfo]) -> bytes:
        data_packet = ClientDataPacket(
            client_id=PacketFactory.client_id,
            city_name=city_name,
            seq_number=PacketFactory.next_seq_number(),
            data=[ClientGatewayPacket(trip_info).encode()]
        )
        trace(f"Built trip packet {city_name}-{data_packet.seq_number}: {min_hash(data_packet.data)}")
        return ClientPacket(data=data_packet).encode()

    @staticmethod
    def build_trip_eof(city: str) -> bytes:
        data_packet = ClientDataPacket(
            client_id=PacketFactory.client_id,
            city_name=city,
            seq_number=PacketFactory.next_seq_number(),
            data=Eof(False)
        )
        return ClientPacket(data=data_packet).encode()
