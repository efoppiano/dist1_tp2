from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class WeatherSideTableInfo(BasicPacket):
    packet_id: str
    city_name: str
    date: str
    prectot: float
