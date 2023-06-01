from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class YearFilterIn(PlainPacket):
    trip_id: str
    city_name: str
    start_station_name: str
    yearid: int
