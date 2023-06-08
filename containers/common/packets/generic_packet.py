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

@dataclass
class PacketIdentifier:
    replica_id: int
    client_id: str
    city_name: str
    packet_id: int

@dataclass
class OverLoadedMessages:
    id_overload: dict
    messages: List[bytes]

def overload( dest: Union[PacketIdentifier, GenericPacket], id_src ):
    if "replica_id" in id_src:
        dest.replica_id = id_src["replica_id"]
    if "client_id" in id_src:
        dest.client_id = id_src["client_id"]
    if "city_name" in id_src:
        dest.city_name = id_src["city_name"]
    if "packet_id" in id_src:
        dest.packet_id = id_src["packet_id"]

    return dest