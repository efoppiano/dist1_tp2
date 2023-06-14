from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class Eof(BasicPacket):
    drop: bool = False
    eviction_time: int = None
