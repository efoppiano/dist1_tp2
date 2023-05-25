from dataclasses import dataclass
from typing import Union

from common.packets.basic_packet import BasicPacket


@dataclass
class Eof(BasicPacket):
    city_name: str
