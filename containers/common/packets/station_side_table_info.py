from dataclasses import dataclass
from typing import Union

from common.packets.basic_packet import BasicPacket


@dataclass
class StationSideTableInfo(BasicPacket):
    packet_id: str
    city_name: str
    station_code: int
    yearid: int
    station_name: str
    latitude: Union[float, None]
    longitude: Union[float, None]
