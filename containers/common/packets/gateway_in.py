from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class GatewayIn(PlainPacket):
    trip_id: str
    city_name: str
    start_datetime: str
    start_station_code: int
    end_datetime: str
    end_station_code: int
    duration_sec: float
    is_member: bool
    yearid: int
