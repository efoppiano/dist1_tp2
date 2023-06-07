import uuid
from dataclasses import dataclass
from typing import Iterator, Union, List

from common.packets.basic_packet import BasicPacket

CHUNK_SIZE = 1024


@dataclass
class WeatherInfo(BasicPacket):
    packet_id: str
    city_name: str
    date: str
    prectot: float
    qv2m: float
    rh2m: float
    ps: float
    t2m_range: float
    ts: float
    t2mdew: float
    t2mwet: float
    t2m_max: float
    t2m_min: float
    t2m: float
    ws50m_range: float
    ws10m_range: float
    ws50m_min: float
    ws10m_min: float
    ws50m_max: float
    ws10m_max: float
    ws50m: float
    ws10m: float

    @staticmethod
    def from_csv(city_name: str, csv_line: str) -> "WeatherInfo":
        packet_id = uuid.uuid4()
        line_data = csv_line.strip().split(",")
        date = line_data[0]
        if len(line_data) == 21:
            line_data = [float(data) for data in line_data[1:-1]]
        else:
            line_data = [float(data) for data in line_data[1:]]

        return WeatherInfo(str(packet_id), city_name, date, *line_data)


@dataclass
class StationInfo(BasicPacket):
    packet_id: str
    city_name: str
    code: int
    name: str
    latitude: Union[float, None]
    longitude: Union[float, None]
    yearid: int

    @staticmethod
    def from_csv(city_name: str, csv_line: str) -> "StationInfo":
        packet_id = uuid.uuid4()
        line_data = csv_line.strip().split(",")
        code = int(line_data[0])
        name = line_data[1]
        latitude = float(line_data[2]) if line_data[2] != "" else None
        longitude = float(line_data[3]) if line_data[3] != "" else None
        yearid = int(line_data[4])
        return StationInfo(
            str(packet_id),
            city_name,
            code,
            name,
            latitude,
            longitude,
            yearid
        )


@dataclass
class TripInfo(BasicPacket):
    trip_id: str
    city_name: str
    start_datetime: str
    start_station_code: int
    end_datetime: str
    end_station_code: int
    duration_sec: float
    is_member: bool
    yearid: int

    @staticmethod
    def from_csv(city_name: str, csv_line: str) -> "TripInfo":
        trip_id = uuid.uuid4()
        line_data = csv_line.strip().split(",")
        start_datetime = line_data[0]
        start_station_code = int(line_data[1])
        end_datetime = line_data[2]
        end_station_code = int(line_data[3])
        duration_sec = float(line_data[4])
        is_member = bool(line_data[5])
        yearid = int(line_data[6])
        return TripInfo(
            str(trip_id),
            city_name,
            start_datetime,
            start_station_code,
            end_datetime,
            end_station_code,
            duration_sec,
            is_member,
            yearid
        )


@dataclass
class ClientEofPacket(BasicPacket):
    file_type: str  # weather, station, trip
    client_id: str
    city_name: str


@dataclass
class ClientGatewayPacket(BasicPacket):
    data: Union[ClientEofPacket, List[WeatherInfo], List[StationInfo], List[TripInfo]]


class WeatherReader:
    def __init__(self, data_folder_path: str, city: str):
        self._data_folder_path = data_folder_path
        self._city = city

    def __weather_path(self) -> str:
        return f"{self._data_folder_path}/{self._city}/weather.csv"

    def next_data(self) -> Iterator[List[WeatherInfo]]:
        with open(self.__weather_path()) as f:
            _ = f.readline()
            weather_info_list = []
            for line in f:
                try:
                    weather_info = WeatherInfo.from_csv(self._city, line)
                except ValueError as e:
                    continue
                weather_info_list.append(weather_info)
                if len(weather_info_list) == CHUNK_SIZE:
                    yield weather_info_list
                    weather_info_list = []
            if len(weather_info_list) > 0:
                yield weather_info_list


class StationReader:
    def __init__(self, data_folder_path: str, city: str):
        self._data_folder_path = data_folder_path
        self._city = city

    def __station_path(self) -> str:
        return f"{self._data_folder_path}/{self._city}/stations.csv"

    def next_data(self) -> Iterator[List[StationInfo]]:
        with open(self.__station_path()) as f:
            _ = f.readline()
            station_info_list = []
            for line in f:
                try:
                    station_info = StationInfo.from_csv(self._city, line)
                except ValueError as e:
                    continue
                station_info_list.append(station_info)
                if len(station_info_list) == CHUNK_SIZE:
                    yield station_info_list
                    station_info_list = []
            if len(station_info_list) > 0:
                yield station_info_list


class TripReader:
    def __init__(self, data_folder_path: str, city: str):
        self._data_folder_path = data_folder_path
        self._city = city

    def __station_path(self) -> str:
        return f"{self._data_folder_path}/{self._city}/trips.csv"

    def next_data(self) -> Iterator[List[TripInfo]]:
        with open(self.__station_path()) as f:
            _ = f.readline()
            trip_info_list = []
            for line in f:
                try:
                    trip_info = TripInfo.from_csv(self._city, line)
                except ValueError as e:
                    continue
                trip_info_list.append(trip_info)
                if len(trip_info_list) == CHUNK_SIZE:
                    yield trip_info_list
                    trip_info_list = []
            if len(trip_info_list) > 0:
                yield trip_info_list
