from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class WeatherSideTableInfo(BasicPacket):
    city_name: str
    date: str
    prectot: float
