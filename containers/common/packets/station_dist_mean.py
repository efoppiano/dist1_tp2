from dataclasses import dataclass

from common.packets.basic_packet import BasicPacket


@dataclass
class StationDistMean(BasicPacket):
    end_station_name: str
    dist_mean: float
    dist_mean_amount: int
