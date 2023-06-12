import logging
import os
import signal
import pickle
import threading

from common.packets.generic_packet import GenericPacket
from common.packets.eof import Eof
from common.packets.client_response_packets import GenericResponsePacket
from common.rabbit_middleware import Rabbit
from common.utils import initialize_log, save_state, load_state, min_hash

SELF_QUEUE = f"sent_responses"
DIST_MEAN_SRC = os.environ["DIST_MEAN_SRC"]
DIST_MEAN_AMOUNT = int(os.environ["DIST_MEAN_AMOUNT"])
TRIP_COUNT_SRC = os.environ["TRIP_COUNT_SRC"]
TRIP_COUNT_AMOUNT = int(os.environ["TRIP_COUNT_AMOUNT"])
DUR_AVG_SRC = os.environ["DUR_AVG_SRC"]
DUR_AVG_AMOUNT = int(os.environ["DUR_AVG_AMOUNT"])


class ResponseProvider:
    def __init__(self):
        self._last_received = {}

        self._rabbit = Rabbit("rabbitmq")
        self.__set_up_signal_handler()
        self.__load_state()
        self.__load_last_sent()

    def __set_up_signal_handler(self):
        def signal_handler(sig, frame):
            if self._sig_hand_prev is not None:
                self._sig_hand_prev(sig, frame)

            logging.info("action: shutdown_response_provider | result: in_progress")
            logging.info("action: shutdown_response_provider | result: success")

        self._sig_hand_prev = signal.signal(signal.SIGTERM, signal_handler)

    def __update_last_received(self, type, packet: GenericPacket):

        sender_id = (type, packet.replica_id)
        current_id = packet.packet_id
        last_id = self._last_received.get(sender_id)

        if current_id == last_id:
            logging.warning(f"Received duplicate {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
            return False
        self._last_received[sender_id] = current_id

        return True

    def __send_response(self, destination: str, message: bytes):

        # TODO: Do not hardcode the queue name
        result_queue = f"results_{destination}"
        self._rabbit.route(SELF_QUEUE, "results", destination)
        self._rabbit.route(result_queue, "results", destination)

        self._rabbit.send_to_route("results", destination, message)

    def __handle_message(self, message: bytes, type: str) -> bool:

        packet = GenericPacket.decode(message)
        if isinstance(packet.data, Eof): type = f"{type}_eof"

        response_packet = GenericResponsePacket(
            packet.client_id, packet.city_name,
            type, packet.replica_id,
            packet.packet_id, packet.data
        )
        response_message = response_packet.encode()

        if not self.__update_last_received(type, packet):
            return True

        try:
            logging.info(f"Sending {response_packet}")
            self.__send_response(packet.client_id, response_message)
        except:
            logging.warning(f"Failed to send {response_packet}")

        self.__save_state()

        return True

    def __handle_type(self, type):
        return lambda message, type=type: self.__handle_message(message, type)

    def __save_state(self):
        save_state(pickle.dumps(self._last_received))

    def __load_state(self):
        try:
            _last_received = pickle.loads(load_state())
            if _last_received is not None:
                self._last_received = _last_received
        except:
            logging.warning("Failed to load state")
            pass

    def __load_last_sent(self):
        self._rabbit.consume_until_empty(SELF_QUEUE, self.__handle_last_sent)

    def __handle_last_sent(self, message: bytes) -> bool:
        packet = GenericResponsePacket.decode(message)
        sender_id = (packet.type, packet.replica_id)

        self._last_received[sender_id] = packet.packet_id

        self.__save_state()

        return True

    def __start(self):

        dist_mean_queue = DIST_MEAN_SRC
        trip_count_queue = TRIP_COUNT_SRC
        avg_queue = DUR_AVG_SRC

        self._rabbit.consume(dist_mean_queue, self.__handle_type("dist_mean"))
        self._rabbit.consume(trip_count_queue, self.__handle_type("trip_count"))
        self._rabbit.consume(avg_queue, self.__handle_type("dur_avg"))

        # Returns True every time, as this is already saved to disk if reading at runtime
        self._rabbit.consume(SELF_QUEUE, lambda _message: True)

        self._rabbit.start()

    def start(self):
        thread = threading.Thread(target=self.__start)
        thread.start()
        thread.join()


def main():
    initialize_log()
    response_provider = ResponseProvider()
    response_provider.start()


if __name__ == "__main__":
    main()
