from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class PrecFilterIn(BasicPacket):
    start_date: str
    duration_sec: float
    prectot: float