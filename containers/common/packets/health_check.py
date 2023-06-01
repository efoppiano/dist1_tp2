from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class HealthCheck(PlainPacket):
    id: str
    timestamp: int
