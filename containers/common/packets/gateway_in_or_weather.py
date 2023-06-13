from dataclasses import dataclass

from typing import Union

from common.packets.basic_packet import BasicPacket
from common.packets.weather_aggregator_trip import StationAggregatorPacket
from common.packets.stop_packet import StopPacket
from common.packets.weather_side_table_info import WeatherSideTableInfo


@dataclass
class GatewayInOrWeather(BasicPacket):
    data: Union[StationAggregatorPacket, WeatherSideTableInfo, StopPacket]
