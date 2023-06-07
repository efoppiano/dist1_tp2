from dataclasses import dataclass

from typing import List, Union

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class GenericPacket(BasicPacket):
    replica_id: int # sender replica id
    client_id: str
    city_name: str
    packet_id: int
    
    data: Union[List[bytes], bytes, Eof]
