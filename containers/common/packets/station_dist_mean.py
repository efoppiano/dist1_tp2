from dataclasses import dataclass

from common.packets.plain_packet import PlainPacket


@dataclass
class StationDistMean(PlainPacket):
    # TODO: check if this could cause problems
    packet_id: str
    city_name: str
    end_station_name: str
    dist_mean: float
