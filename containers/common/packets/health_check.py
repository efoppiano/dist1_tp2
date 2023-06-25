from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class HealthCheck(BasicPacket):
    id: str
    timestamp: int
    max_lapse_ns: int = 0
