from dataclasses import dataclass
from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof
from common.utils import min_hash


@dataclass
class GenericPacket(BasicPacket):
    sender_id: str
    client_id: str
    city_name: str
    seq_number: int
    maybe_dup: bool

    data: Union[List[bytes], Eof]
    hash: str = None

    def is_eof(self) -> bool:
        return isinstance(self.data, Eof)

    def is_chunk(self) -> bool:
        return isinstance(self.data, list)

    def get_id(self) -> str:
        return f"{self.client_id}-{self.city_name}-{self.seq_number}"

    def get_flow_id(self) -> str:
        return f"{self.client_id}-{self.city_name}"

    def data_hash(self) -> str:
        return min_hash(self.data)

    def encode(self) -> bytes:
        self.hash = self.data_hash()
        return super().encode()


class GenericPacketBuilder:
    def __init__(self, sender_id: str, client_id: str, city_name: str):
        self._sender_id = sender_id
        self._client_id = client_id
        self._city_name = city_name

    def build(self, seq_number: int, maybe_dup: bool, data: Union[List[bytes], Eof]) -> GenericPacket:
        return GenericPacket(
            sender_id=self._sender_id,
            client_id=self._client_id,
            city_name=self._city_name,
            seq_number=seq_number,
            maybe_dup=maybe_dup,
            data=data
        )

    def get_id(self) -> str:
        return f"{self._sender_id}-{self._client_id}-{self._city_name}"


@dataclass
class PacketIdentifier:
    replica_id: int
    client_id: str
    city_name: str
    packet_id: Union[int, tuple]
