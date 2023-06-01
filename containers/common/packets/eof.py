from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class Eof(PlainPacket):
    city_name: str
