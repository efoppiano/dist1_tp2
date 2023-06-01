from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class DistanceCalcIn(PlainPacket):
    trip_id: str
    city_name: str

    start_station_name: str
    start_station_latitude: float
    start_station_longitude: float

    end_station_name: str
    end_station_latitude: float
    end_station_longitude: float
