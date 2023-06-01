from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class PrecFilterIn(PlainPacket):
    trip_id: str
    city_name: str
    start_date: str
    duration_sec: float
    prectot: float