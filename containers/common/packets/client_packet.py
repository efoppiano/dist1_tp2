from dataclasses import dataclass
from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class ClientDataPacket(BasicPacket):
    client_id: str
    city_name: str
    seq_number: int
    data: Union[List[bytes], Eof]

    def is_eof(self):
        return isinstance(self.data, Eof)

    def is_chunk(self):
        return not self.is_eof()

    def get_id(self) -> str:
        return f"{self.client_id}-{self.city_name}-{self.seq_number}"

    def get_flow_id(self) -> str:
        return f"{self.client_id}-{self.city_name}"


@dataclass
class ClientPacket(BasicPacket):
    data: Union[ClientDataPacket, str]
