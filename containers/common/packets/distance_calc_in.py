from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class DistanceCalcIn(BasicPacket):
    trip_id: str
    city_name: str

    start_station_name: str
    start_station_latitude: float
    start_station_longitude: float

    end_station_name: str
    end_station_latitude: float
    end_station_longitude: float
