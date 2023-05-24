from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class DistInfo(BasicPacket):
    city_name: str
    end_station_name: str
    distance_km: float
