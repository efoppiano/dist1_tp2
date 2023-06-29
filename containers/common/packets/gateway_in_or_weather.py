from dataclasses import dataclass

from typing import Union

from common.packets.basic_packet import BasicPacket
from common.packets.gateway_in import GatewayIn
from common.packets.weather_side_table_info import WeatherSideTableInfo


@dataclass
class GatewayInOrWeather(BasicPacket):
    data: Union[GatewayIn, WeatherSideTableInfo]
