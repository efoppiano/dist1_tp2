from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class DistInfo(PlainPacket):
    trip_id: str
    city_name: str
    end_station_name: str
    distance_km: float
