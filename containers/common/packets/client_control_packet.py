from dataclasses import dataclass
from typing import Union, Literal

from common.packets.basic_packet import BasicPacket
from common.packets.basic_packet import BasicPacket


@dataclass
class RateLimitChangeRequest(BasicPacket):
    new_rate: int


@dataclass
class ClientControlPacket(BasicPacket):
    data: Union[str, RateLimitChangeRequest]
