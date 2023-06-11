import os
import time
from typing import Dict, List, Union
from pickle import dumps, loads
from common.basic_stateful_filter import BasicStatefulFilter
from common.packets.eof_with_id import EofWithId
from common.packets.gateway_in import GatewayIn
from common.packets.gateway_in_or_weather import GatewayInOrWeather
from common.packets.gateway_out_or_station import GatewayOutOrStation
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.stop_packet import StopPacket
from common.packets.weather_side_table_info import WeatherSideTableInfo
from common.readers import ClientGatewayPacket, ClientIdPacket, ClientEofPacket, StationInfo, WeatherInfo, TripInfo
from common.utils import initialize_log

REPLICA_ID = os.environ["REPLICA_ID"]
WEATHER_SIDE_TABLE_QUEUE_NAME = os.environ["WEATHER_SIDE_TABLE_QUEUE_NAME"]
STATION_SIDE_TABLE_QUEUE_NAME = os.environ["STATION_SIDE_TABLE_QUEUE_NAME"]


class Gateway(BasicStatefulFilter):
  def __init__(self, replica_id: int, weather_side_table_queue_name: str, station_side_table_queue_name: str):

    self._replica_id = replica_id
    self._weather_side_table_queue_name = weather_side_table_queue_name
    self._station_side_table_queue_name = station_side_table_queue_name

    super().__init__(replica_id)

  def __handle_client_eof(self, flow_id, packet: ClientEofPacket) -> Dict[str, List[bytes]]:

    if packet.file_type == "weather":
      return {
          self._weather_side_table_queue_name: [
              GatewayInOrWeather(StopPacket(packet.city_name)).encode()]
      }
    elif packet.file_type == "station":
      return {
          self._station_side_table_queue_name: [
              GatewayOutOrStation(StopPacket(packet.city_name)).encode()]
      }
    elif packet.file_type == "trip":
      queue_name = Linker().get_eof_in_queue(self)
      return {
          # The replica_id here is not relevant, because only one gateway can
          # handle data of a city
          queue_name: [
              EofWithId(packet.client_id, packet.city_name,
                        self._replica_id).encode()
          ]
      }
    else:
      raise ValueError(f"Unknown file type: {packet.file_type}")

  def __handle_client_id_req(self, flow_id, packet: ClientIdPacket) -> Dict[str, List[bytes]]:

    new_client_id = f"{self._replica_id}_{time.time_ns()}"
    response = ClientIdPacket(new_client_id).encode()

    return {
        # TODO: Do not hardcode the queue name
        "client_id_queue": [response]
    }

  def __handle_list(self, flow_id, packet: List[Union[WeatherInfo, StationInfo, TripInfo]]) -> Dict[str, List[bytes]]:
    if len(packet) == 0:
      return {}
    element_type = type(packet[0])
    if element_type == WeatherInfo:
      packets_to_send = []
      for weather_info in packet:
        packets_to_send.append(
            GatewayInOrWeather(
                WeatherSideTableInfo(weather_info.date, weather_info.prectot)).encode())
      return {
          self._weather_side_table_queue_name: packets_to_send
      }
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
      return {
          self._station_side_table_queue_name: packets_to_send
      }
    elif element_type == TripInfo:
      queue_name = Linker().get_output_queue(
          self, hashing_key=packet[0].start_datetime)
      packets_to_send = []
      for t in packet:
        gateway_in = GatewayIn(
            t.start_datetime,
            t.start_station_code, t.end_datetime,
            t.end_station_code, t.duration_sec, t.is_member,
            t.yearid
        )
        packets_to_send.append(GatewayInOrWeather(gateway_in).encode())

      return {
          queue_name: packets_to_send
      }
    else:
      raise ValueError(f"Unknown packet type: {element_type}")

  def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
    packet = ClientGatewayPacket.decode(message)

    if isinstance(packet.data, ClientEofPacket):
      return self.__handle_client_eof(flow_id, packet.data)
    elif isinstance(packet.data, ClientIdPacket):
      return self.__handle_client_id_req(flow_id, packet.data)
    elif isinstance(packet.data, list):
      return self.__handle_list(flow_id, packet.data)
    else:
      raise ValueError(f"Unknown packet type: {type(packet.data)}")


def main():
  initialize_log()
  gateway = Gateway(int(REPLICA_ID), WEATHER_SIDE_TABLE_QUEUE_NAME,
                    STATION_SIDE_TABLE_QUEUE_NAME)
  gateway.start()


if __name__ == "__main__":
  main()
