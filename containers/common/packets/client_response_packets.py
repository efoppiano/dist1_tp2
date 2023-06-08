from dataclasses import dataclass

from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class GenericResponsePacket(BasicPacket):
    flow_id: tuple
    case_id: tuple
    packet_id: int
    type: str
    data: Union[List[bytes], Eof]
