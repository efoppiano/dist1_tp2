from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class EofWithId(BasicPacket):
    city_name: str
    replica_id: int
