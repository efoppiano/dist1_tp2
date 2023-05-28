import logging
import signal
import threading
from typing import List

import zmq

from common.packet_factory import DIST_MEAN_REQUEST, TRIP_COUNT_REQUEST, DUR_AVG_REQUEST
from common.rabbit_middleware import Rabbit
from common.utils import initialize_log, build_eof_out_queue_name


class ResponseProvider:
    def __init__(self, zmq_addr: str, cities: List[str]):
        self._zmq_addr = zmq_addr
        self._cities = cities
        self._context = zmq.Context()

        self._rabbit = Rabbit("rabbitmq", use_heartbeat=False)
        self.__set_up_signal_handler()

    def __set_up_signal_handler(self):
        def signal_handler(sig, frame):
            if self._sig_hand_prev is not None:
                self._sig_hand_prev(sig, frame)

            logging.info("action: shutdown_response_provider | result: in_progress")
            self._context.term()
            logging.info("action: shutdown_response_provider | result: success")

        self._sig_hand_prev = signal.signal(signal.SIGTERM, signal_handler)

    @staticmethod
    def __send_dist_mean_response(socket: zmq.Socket, response: bytes) -> bool:
        socket.send(response)

        return True

    @staticmethod
    def __send_trip_count_response(socket: zmq.Socket, response: bytes) -> bool:
        socket.send(response)

        return True

    @staticmethod
    def __send_avg_response(socket: zmq.Socket, response: bytes) -> bool:
        socket.send(response)
        return True

    def __start(self):
        socket = self._context.socket(zmq.REP)
        socket.setsockopt(zmq.LINGER, 0)
        socket.bind(self._zmq_addr)

        self._rabbit.route("dist_mean_provider", "control", build_eof_out_queue_name("dist_mean_provider"))
        self._rabbit.route("trip_count_provider", "control", build_eof_out_queue_name("trip_count_provider"))
        self._rabbit.route("avg_provider", "control", build_eof_out_queue_name("avg_provider"))
        try:
            while True:
                message = bytes(socket.recv())
                if message == DIST_MEAN_REQUEST:
                    self._rabbit.consume_one("dist_mean_provider",
                                             lambda response, socket=socket: self.__send_dist_mean_response(socket,
                                                                                                            response))
                elif message == TRIP_COUNT_REQUEST:
                    self._rabbit.consume_one("trip_count_provider",
                                             lambda response, socket=socket: self.__send_trip_count_response(socket,
                                                                                                             response))
                elif message == DUR_AVG_REQUEST:
                    self._rabbit.consume_one("avg_provider",
                                             lambda response, socket=socket: self.__send_avg_response(socket, response))
        except Exception as e:
            logging.info("action: shutdown_provider_start | result: interrupted")
            raise e
        finally:
            socket.close()

    def start(self):
        thread = threading.Thread(target=self.__start)
        thread.start()
        thread.join()


def main():
    initialize_log(logging.INFO)
    cities = ["montreal", "toronto", "washington"]
    response_provider = ResponseProvider("tcp://0.0.0.0:5555", cities)
    response_provider.start()


if __name__ == "__main__":
    main()
