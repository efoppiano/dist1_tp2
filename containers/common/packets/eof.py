from dataclasses import dataclass

from typing import Union

from common.packets.basic_packet import BasicPacket


@dataclass
class Eof(BasicPacket):
    drop: bool = False
    eviction_time: Union[int, None] = None
