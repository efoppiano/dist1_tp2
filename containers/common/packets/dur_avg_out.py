from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class DurAvgOut(BasicPacket):
    start_date: str
    dur_avg_sec: float
    dur_avg_amount: int
