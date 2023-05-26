from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


# TODO: Use this packet instead of Eof in filters and aggregators
@dataclass
class EofWithId(BasicPacket):
    city_name: str
    replica_id: int
