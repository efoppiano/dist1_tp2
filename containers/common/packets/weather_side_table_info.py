from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class WeatherSideTableInfo(BasicPacket):
    date: str
    prectot: float
