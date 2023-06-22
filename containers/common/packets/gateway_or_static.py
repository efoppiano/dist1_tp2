from dataclasses import dataclass

from typing import Union

from common.packets.basic_packet import BasicPacket
from common.packets.gateway_out import GatewayOut
from common.packets.gateway_in import GatewayIn
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.packets.stop_packet import StopPacket


@dataclass
class GatewayOrStatic(BasicPacket):
    data: Union[GatewayOut, GatewayIn, StationSideTableInfo, WeatherSideTableInfo, StopPacket]


def is_side_table_message(message_bytes: bytes) -> bool:
    data = GatewayOrStatic.decode(message_bytes).data
    return isinstance(data, StationSideTableInfo) or isinstance(data, WeatherSideTableInfo)