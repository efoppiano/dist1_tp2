from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class DurAvgOut(BasicPacket):
    packet_id: str
    city_name: str
    start_date: str
    dur_avg_sec: float
