from dataclasses import dataclass

from typing import Union, List

from common.packets.basic_packet import BasicPacket
from common.packets.dur_avg_out import DurAvgOut
from common.packets.eof import Eof
from common.packets.station_dist_mean import StationDistMean
from common.packets.trips_count_by_year_joined import TripsCountByYearJoined


@dataclass
class GenericResponsePacket(BasicPacket):
    replica_id: int
    type: str
    data: Union[List[bytes], Eof]
