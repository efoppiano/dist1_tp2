from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class TripsCountByYearJoined(BasicPacket):
    packet_id: str
    city_name: str
    start_station_name: str
    trips_16: int
    trips_17: int
