from dataclasses import dataclass
from common.packets.basic_packet import BasicPacket

@dataclass
class ClientIdPacket(BasicPacket):
    '''Works both as request and response'''
    client_id: str