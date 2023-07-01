from dataclasses import dataclass

from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class GenericResponsePacket(BasicPacket):
    client_id: str
    city_name: str
    type: str
    sender_id: str
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
