from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class DistInfo(BasicPacket):
    end_station_name: str
    distance_km: float
