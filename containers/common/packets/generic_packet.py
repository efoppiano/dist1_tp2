from dataclasses import dataclass

from typing import List, Union

from common.packets.basic_packet import BasicPacket
from common.packets.eof import Eof


@dataclass
class GenericPacket(BasicPacket):
    data: Union[List[bytes], bytes, Eof]
