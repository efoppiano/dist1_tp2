from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class FullStationSideTableInfo(BasicPacket):
    station_code: int
    yearid: int
    station_name: str
    latitude: float
    longitude: float
