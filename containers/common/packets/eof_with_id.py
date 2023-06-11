from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class EofWithId(BasicPacket):
    client_id: str
    city_name: str
    replica_id: int
