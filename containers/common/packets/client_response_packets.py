from dataclasses import dataclass

from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class GenericResponsePacket(BasicPacket):
    replica_id: int
    type: str
    data: Union[List[bytes], Eof]
