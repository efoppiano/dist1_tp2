from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class TripsCountByYearJoined(PlainPacket):
    packet_id: str
    city_name: str
    start_station_name: str
    trips_16: int
    trips_17: int
