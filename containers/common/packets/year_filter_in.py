from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class YearFilterIn(BasicPacket):
    start_station_name: str
    yearid: int
