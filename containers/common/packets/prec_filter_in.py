from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class PrecFilterIn(BasicPacket):
    trip_id: str
    city_name: str
    start_date: str
    duration_sec: float
    prectot: float