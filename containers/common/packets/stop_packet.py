from dataclasses import dataclass

from typing import Union, List

from common.packets.basic_packet import BasicPacket


@dataclass
class StopPacket(BasicPacket):
    city_name: str
