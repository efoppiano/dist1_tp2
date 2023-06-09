import os
from typing import Dict, List, Union

from basic_gateway import BasicGateway
from common.components.message_sender import OutgoingMessages
from common.packets.gateway_in_or_weather import GatewayInOrWeather
from common.packets.gateway_out_or_station import GatewayOutOrStation
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.gateway_in import GatewayIn
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.components.readers import ClientGatewayPacket, StationInfo, WeatherInfo, TripInfo
from common.utils import initialize_log

WEATHER_SIDE_TABLE_QUEUE_NAME = os.environ["WEATHER_SIDE_TABLE_QUEUE_NAME"]
STATION_SIDE_TABLE_QUEUE_NAME = os.environ["STATION_SIDE_TABLE_QUEUE_NAME"]


class Gateway(BasicGateway):
    def __init__(self, weather_side_table_queue_name: str, station_side_table_queue_name: str):
        self._weather_side_table_queue_name = weather_side_table_queue_name
        self._station_side_table_queue_name = station_side_table_queue_name

        super().__init__()

    def __handle_list(self, flow_id, packet: List[Union[WeatherInfo, StationInfo, TripInfo]]) -> OutgoingMessages:
        if len(packet) == 0:
            return OutgoingMessages({})
        element_type = type(packet[0])
        if element_type == WeatherInfo:
            packets_to_send = []
            for weather_info in packet:
                packets_to_send.append(
                    GatewayInOrWeather(
                        WeatherSideTableInfo(weather_info.date, weather_info.prectot)).encode())
            return OutgoingMessages({
                self._weather_side_table_queue_name: packets_to_send
            })
        elif element_type == StationInfo:
            packets_to_send = []
            for station_info in packet:
                packets_to_send.append(
                    GatewayOutOrStation(
                        StationSideTableInfo(
                            station_info.code,
                            station_info.yearid,
                            station_info.name, station_info.latitude,
                            station_info.longitude
                        )
                    ).encode()
                )
            return OutgoingMessages({
                self._station_side_table_queue_name: packets_to_send
            })
        elif element_type == TripInfo:
            queue_name = self.router.route(packet[0].start_datetime)
            packets_to_send = []
            for t in packet:
                gateway_in = GatewayIn(
                    t.start_datetime,
                    t.start_station_code, t.end_datetime,
                    t.end_station_code, t.duration_sec, t.is_member,
                    t.yearid
                )
                packets_to_send.append(GatewayInOrWeather(gateway_in).encode())

            return OutgoingMessages({
                queue_name: packets_to_send
            })
        else:
            raise ValueError(f"Unknown packet type: {element_type}")

    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        packet = ClientGatewayPacket.decode(message)

        if isinstance(packet.data, list):
            return self.__handle_list(flow_id, packet.data)
        else:
            raise ValueError(f"Unknown packet type: {type(packet.data)}")


def main():
    initialize_log(15)
    gateway = Gateway(WEATHER_SIDE_TABLE_QUEUE_NAME,
                      STATION_SIDE_TABLE_QUEUE_NAME)
    gateway.start()


if __name__ == "__main__":
    main()
