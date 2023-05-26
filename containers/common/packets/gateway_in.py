from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class GatewayIn(BasicPacket):
    trip_id: int
    city_name: str
    start_datetime: str
    start_station_code: int
    end_datetime: str
    end_station_code: int
    duration_sec: float
    is_member: bool
    yearid: int
