from dataclasses import dataclass

from typing import Union

from common.packets.basic_packet import BasicPacket
from common.packets.gateway_out import GatewayOut
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.stop_packet import StopPacket


@dataclass
class GatewayOutOrStation(BasicPacket):
    data: Union[GatewayOut, StationSideTableInfo, StopPacket]
