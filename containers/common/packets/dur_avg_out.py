from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class DurAvgOut(PlainPacket):
    packet_id: str
    city_name: str
    start_date: str
    dur_avg_sec: float
    dur_avg_amount: int
