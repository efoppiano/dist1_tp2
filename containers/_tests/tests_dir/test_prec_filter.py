import os
import unittest

os.environ["PREC_LIMIT"] = "None"
os.environ["REPLICA_ID"] = "None"

from common.basic_stateful_filter import BasicStatefulFilter
from common.packets.prec_filter_in import PrecFilterIn
from prec_filter.prec_filter import PrecFilter


class TestPrecFilter(unittest.TestCase):
    def setUp(self) -> None:
        BasicStatefulFilter.__init__ = lambda self, *args, **kwargs: None

    def __build_packet(self, trip_id: str, city_name: str, start_date: str, duration_sec: float,
                       prectot: float) -> bytes:
        return PrecFilterIn(trip_id, city_name, start_date, duration_sec, prectot).encode()

    def test_prec_filter_should_filter_out_packets_with_low_precipitation(self):
        prec_filter = PrecFilter(0, 30)
        packet = self.__build_packet("abc123", "washington", "2019-04-20", 25, 10)
        result = prec_filter.handle_message(packet)
        self.assertEqual(result, {})

    def test_prec_filter_should_not_filter_out_packets_with_high_precipitation(self):
        prec_filter = PrecFilter(0, 30)
        packet = self.__build_packet("abc123", "washington", "2019-04-20", 25, 40)
        result = prec_filter.handle_message(packet)
        self.assertEqual(len(result), 1)

    def test_prec_filter_limit_should_change_if_parameter_changes(self):
        prec_filter = PrecFilter(0, 50)
        packet = self.__build_packet("abc123", "washington", "2019-04-20", 25, 40)
        result = prec_filter.handle_message(packet)
        self.assertEqual(result, {})

    def test_prec_filter_should_return_the_same_packet(self):
        prec_filter = PrecFilter(0, 30)
        packet = self.__build_packet("abc123", "washington", "2019-04-20", 25, 40)
        result = prec_filter.handle_message(packet)
        self.assertEqual(len(list(result.values())), 1)
        self.assertEqual(list(result.values())[0], [packet])

    def test_prec_filter_filters_packet_with_prec_equal_to_limit(self):
        prec_filter = PrecFilter(0, 30)
        packet = self.__build_packet("abc123", "washington", "2019-04-20", 25, 30)
        result = prec_filter.handle_message(packet)
        self.assertEqual(result, {})

    def test_prec_filter_packets_with_the_same_start_date_go_to_the_same_queue(self):
        prec_filter = PrecFilter(0, 10)
        packet_1 = self.__build_packet("abc123", "washington", "2019-04-20", 25, 30)
        packet_2 = self.__build_packet("653-lop", "toronto", "2019-04-20", 243, 54)

        result_1 = prec_filter.handle_message(packet_1)
        result_2 = prec_filter.handle_message(packet_2)

        queue_1 = list(result_1.keys())[0]
        queue_2 = list(result_2.keys())[0]

        self.assertEqual(queue_1, queue_2)
