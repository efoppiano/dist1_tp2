import logging
import os
import signal
import pickle

from common.components.heartbeater.heartbeater import HeartBeater
from common.packets.generic_packet import GenericPacket
from common.packets.eof import Eof
from common.packets.client_response_packets import GenericResponsePacket
from common.middleware.rabbit_middleware import Rabbit
from common.utils import initialize_log, save_state, load_state, min_hash, log_duplicate, log_evict, trace

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
        self._eofs_received = {}
        self._evicting = {}

        self.input_queues = {
            "dist_mean": (DIST_MEAN_SRC, DIST_MEAN_AMOUNT),
            "trip_count": (TRIP_COUNT_SRC, TRIP_COUNT_AMOUNT),
            "dur_avg": (DUR_AVG_SRC, DUR_AVG_AMOUNT),
        }

        self._rabbit = Rabbit("rabbitmq")
        self._heartbeater = HeartBeater()
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

    def __update_last_received(self, packet_type, packet: GenericPacket):
        sender_id = (packet_type, packet.sender_id)

        current_id = packet.get_id()
        self._last_received.setdefault(sender_id, [None, None])
        last_chunk_id, last_eof_id = self._last_received[sender_id]

        if packet.is_eof():
            if current_id == last_eof_id:
                log_duplicate(
                    f"Received duplicate EOF {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_received[sender_id][1] = current_id
        elif packet.is_chunk():
            if current_id == last_chunk_id:
                log_duplicate(
                    f"Received duplicate chunk {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_received[sender_id][0] = current_id

        logging.debug(
            f"Received {sender_id}-{current_id}-{min_hash(packet.data)}")

        return True

    def __send_response(self, destination: str, message: bytes):

        # TODO: Do not hardcode the queue name
        result_queue = f"results_{destination}"
        self._rabbit.route(SELF_QUEUE, "results", destination)
        self._rabbit.route(result_queue, "results", destination)

        self._rabbit.send_to_route("results", destination, message)

    def __evict_client(self, client_id: str, time: int = 0):
        if client_id in self._evicting and not time == 0:
            return

        if time < 1:
            _time = self._evicting.get(client_id, 0)
            log_evict(f"Evicting client {client_id} after {_time} seconds")
            self._rabbit.delete_queue(f"results_{client_id}")
            self._rabbit.delete_queue(f"control_{client_id}")
        else:
            log_evict(f"Evicting client {client_id} in {time} seconds")
            self._rabbit.call_later(time, lambda client_id=client_id: self.__evict_client(client_id))

        self._evicting[client_id] = time

    def __handle_eof(self, packet: GenericPacket, eof: Eof, packet_type: str) -> bool:
        flow_id = (packet.client_id, packet.city_name, packet_type)
        eof_key = (flow_id, eof.timestamp)
        self._eofs_received.setdefault(eof_key, 0)
        self._eofs_received[eof_key] += 1

        if self._eofs_received[eof_key] < self.input_queues[packet_type][1]:
            trace(
                f"Received EOF {eof_key} - {self._eofs_received[eof_key]}/{self.input_queues[packet_type][1]}")
            self.__save_state()
            return False
        logging.debug(
            f"Received EOF {eof_key} - {self._eofs_received[eof_key]}/{self.input_queues[packet_type][1]}")

        self._eofs_received.pop(eof_key)

        if eof.drop:
            self.__evict_client(packet.client_id)
            self.__save_state()
            return False
        elif eof.eviction_time is not None:
            self.__evict_client(packet.client_id, eof.eviction_time)
            return True

        return True

    def __handle_message(self, message: bytes, packet_type: str) -> bool:

        packet = GenericPacket.decode(message)

        if not self.__update_last_received(packet_type, packet):
            return True

        if isinstance(packet.data, Eof):
            if not self.__handle_eof(packet, packet.data, packet_type):
                return True

        response_packet = GenericResponsePacket(
            packet.client_id, packet.city_name,
            packet_type, packet.sender_id,
            packet.seq_number, packet.data
        )
        response_message = response_packet.encode()

        try:
            logging.info(f"Sending {response_packet.client_id}-{response_packet.city_name}-{packet_type}")
            self.__send_response(packet.client_id, response_message)
        except:
            logging.warning(f"Failed to send {response_packet.client_id}-{response_packet.city_name}-{packet_type}")

        self.__save_state()
        return True

    def __handle_type(self, type):
        return lambda message, type=type: self.__handle_message(message, type)

    def __save_state(self):
        state = {
            "_last_received": self._last_received,
            "_eofs_received": self._eofs_received,
            "_evicting": self._evicting,
        }
        save_state(pickle.dumps(state))

    def __load_state(self):
        state_bytes = load_state()
        if state_bytes is None:
            return

        state = pickle.loads(state_bytes)
        if state is not None:
            self._last_received = state["_last_received"]
            self._eofs_received = state["_eofs_received"]
            self._evicting = state["_evicting"]

    def __load_last_sent(self):
        self._rabbit.consume_until_empty(SELF_QUEUE, self.__handle_last_sent)

    def __handle_last_sent(self, message: bytes) -> bool:
        packet = GenericResponsePacket.decode(message)
        sender_id = (packet.type, packet.sender_id)

        self._last_received[sender_id] = packet.packet_id

        self.__save_state()

        return True

    def start(self):

        dist_mean_queue = self.input_queues["dist_mean"][0]
        trip_count_queue = self.input_queues["trip_count"][0]
        avg_queue = self.input_queues["dur_avg"][0]

        self._rabbit.consume(dist_mean_queue, self.__handle_type("dist_mean"))
        self._rabbit.consume(trip_count_queue, self.__handle_type("trip_count"))
        self._rabbit.consume(avg_queue, self.__handle_type("dur_avg"))

        self._rabbit.route(dist_mean_queue, "publish", dist_mean_queue)
        self._rabbit.route(trip_count_queue, "publish", trip_count_queue)
        self._rabbit.route(avg_queue, "publish", avg_queue)

        # Returns True every time, as this is already saved to disk if reading at runtime
        self._rabbit.consume(SELF_QUEUE, lambda _message: True)

        self._heartbeater.start()

        self._rabbit.start()


def main():
    initialize_log()
    response_provider = ResponseProvider()
    response_provider.start()


if __name__ == "__main__":
    main()
