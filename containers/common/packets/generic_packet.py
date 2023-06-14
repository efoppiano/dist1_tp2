from dataclasses import dataclass
from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class GenericPacket(BasicPacket):
    replica_id: int  # sender replica id
    client_id: str
    city_name: str
    seq_number: int

    data: Union[List[bytes], Eof]

    def is_eof(self) -> bool:
        return isinstance(self.data, Eof)

    def is_chunk(self) -> bool:
        return isinstance(self.data, list)

    def get_id(self) -> str:
        return f"{self.client_id}-{self.city_name}-{self.seq_number}"

    def get_flow_id(self) -> str:
        return f"{self.client_id}-{self.city_name}"


class GenericPacketBuilder:
    def __init__(self, replica_id: int, client_id: str, city_name: str):
        self._replica_id = replica_id
        self._client_id = client_id
        self._city_name = city_name

    def build(self, seq_number: int, data: Union[List[bytes], Eof]) -> GenericPacket:
        return GenericPacket(
            replica_id=self._replica_id,
            client_id=self._client_id,
            city_name=self._city_name,
            seq_number=seq_number,
            data=data
        )

    def get_id(self) -> str:
        return f"{self._replica_id}-{self._client_id}-{self._city_name}"


@dataclass
class PacketIdentifier:
    replica_id: int
    client_id: str
    city_name: str
    packet_id: Union[int, tuple]
