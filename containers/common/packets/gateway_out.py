from dataclasses import dataclass


from common.packets.plain_packet import PlainPacket


@dataclass
class GatewayOut(PlainPacket):
    trip_id: str
    city_name: str
    start_date: str
    start_station_code: int
    end_station_code: int
    duration_sec: float
    yearid: int
    prectot: float
