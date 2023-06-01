from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class WeatherSideTableInfo(PlainPacket):
    packet_id: str
    city_name: str
    date: str
    prectot: float
