from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class EofWithId(PlainPacket):
    city_name: str
    replica_id: int
