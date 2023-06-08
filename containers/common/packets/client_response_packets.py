from dataclasses import dataclass

from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class GenericResponsePacket(BasicPacket):
    client_id: str
    city_name: str
    type: str
    replica_id: int
    packet_id: int
    data: Union[List[bytes], Eof]
