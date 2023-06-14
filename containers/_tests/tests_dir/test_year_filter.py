import os
import unittest

from common.packets.year_filter_in import YearFilterIn
from year_filter.year_filter import YearFilter

os.environ["REPLICA_ID"] = "None"

from common.basic_classes.basic_stateful_filter import BasicStatefulFilter


class TestYearFilter(unittest.TestCase):
    def setUp(self) -> None:
        BasicStatefulFilter.__init__ = lambda self, *args, **kwargs: None

    def __build_packet(self, trip_id: str, city_name: str, start_station_name: str, yearid: int) -> bytes:
        return YearFilterIn(trip_id, city_name, start_station_name, yearid).encode()

    def test_year_filter_filters_packet_from_year_before_2016(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("abc123", "washington", "station1", 2015)
        result = year_filter.handle_message(packet)
        self.assertEqual(result, {})

    def test_year_filter_filters_packet_from_year_after_2017(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("abc123", "washington", "station1", 2018)
        result = year_filter.handle_message(packet)
        self.assertEqual(result, {})

    def test_year_filter_does_not_filter_packet_from_year_2016(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("abc123", "washington", "station1", 2016)
        result = year_filter.handle_message(packet)
        self.assertEqual(len(result), 1)

    def test_year_filter_does_not_filter_packet_from_year_2017(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("abc123", "washington", "station1", 2017)
        result = year_filter.handle_message(packet)
        self.assertEqual(len(result), 1)

    def test_year_filter_does_not_filter_packet_from_year_2016_with_different_trip_id(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("another_trip_id", "washington", "station1", 2016)
        result = year_filter.handle_message(packet)
        self.assertEqual(len(result), 1)

    def test_year_filter_does_not_filter_packet_from_year_2016_with_different_city_name(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("abc123", "another_city", "station1", 2016)
        result = year_filter.handle_message(packet)
        self.assertEqual(len(result), 1)

    def test_year_filter_does_not_filter_packet_from_year_2016_with_different_start_station_name(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("abc123", "washington", "another_station", 2016)
        result = year_filter.handle_message(packet)
        self.assertEqual(len(result), 1)

    def test_year_filter_should_return_the_same_packet(self):
        year_filter = YearFilter(0)
        packet = self.__build_packet("abc123", "washington", "station1", 2016)
        result = year_filter.handle_message(packet)
        self.assertEqual(len(list(result.values())), 1)
        self.assertEqual(list(result.values())[0], [packet])

    def test_year_filter_packets_with_the_same_start_station_name_go_to_the_same_queue(self):
        year_filter = YearFilter(0)
        packet_1 = self.__build_packet("abc123", "washington", "My Station", 2016)
        packet_2 = self.__build_packet("fgh987", "montreal", "My Station", 2017)

        result_1 = year_filter.handle_message(packet_1)
        result_2 = year_filter.handle_message(packet_2)

        queue_1 = list(result_1.keys())[0]
        queue_2 = list(result_2.keys())[0]

        self.assertEqual(queue_1, queue_2)
