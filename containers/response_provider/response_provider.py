import logging
import os
import signal
import threading
from typing import List

from common.packets.generic_packet import GenericPacket
from common.packet_factory import DIST_MEAN_REQUEST, TRIP_COUNT_REQUEST, DUR_AVG_REQUEST
from common.rabbit_middleware import Rabbit
from common.utils import initialize_log, build_eof_out_queue_name, hash_msg

REPLICA_ID = os.environ["REPLICA_ID"]

class ResponseProvider:
    def __init__(self, replica_id: int):
        self._replica_id = replica_id

        self._last_hash_by_replica = {
            "dist_mean": {},
            "trip_count": {},
            "avg": {},
            "dist_mean_eof": {},
            "trip_count_eof": {},
            "avg_eof": {},
        }

        self._rabbit = Rabbit("rabbitmq")
        self.__set_up_signal_handler()
        self.__load_state()

    def __set_up_signal_handler(self):
        def signal_handler(sig, frame):
            if self._sig_hand_prev is not None:
                self._sig_hand_prev(sig, frame)

            logging.info("action: shutdown_response_provider | result: in_progress")
            logging.info("action: shutdown_response_provider | result: success")

        self._sig_hand_prev = signal.signal(signal.SIGTERM, signal_handler)

    def __update_last_hash(self, type: str, replica_id: int, new_hash: str) -> bool:

      last_hash = self._last_hash_by_replica[type].get(replica_id)
      if last_hash == new_hash:
        return False
      
      self._last_hash_by_replica[type][replica_id] = new_hash
      return True

    def __handle_message(self, message: bytes, type: str) -> bool:

      packet = GenericPacket.decode(message)

      logging.info(packet)
      if not self.__update_last_hash(type, packet.replica_id, hash_msg(message)):
        logging.warning(f"Received duplicate message from replica {packet.replica_id} - ignoring")
        return True
      

      # TODO: Produce to different <client_id>_<city_id> queues or <client_id>_<city_id>_<type>
      # TODO: Atomically produce for self
      self._rabbit.produce("results", message)

      self.__save_state(type)
      # TODO

      return True
      
    def __handle_type(self, type):
        return lambda message, type=type: self.__handle_message(message, type)
    
    def __save_state(self, type: str):
       # TODO: Save state to disk
       pass
    
    def __load_state(self):
       # TODO: Load state from disk
       # TODO: Load last hash from self queue
       pass

    def __start(self):

        dist_mean_queue = f"dist_mean_provider_{REPLICA_ID}"
        trip_count_queue = f"trip_count_provider_{REPLICA_ID}"
        avg_queue = f"avg_provider_{REPLICA_ID}"

        self._rabbit.consume(dist_mean_queue, self.__handle_type("dist_mean"))
        self._rabbit.consume(trip_count_queue, self.__handle_type("trip_count"))
        self._rabbit.consume(avg_queue, self.__handle_type("avg"))

        # TODO
        # Q: What does this do without a callback ?
        # Q: Is there a guarantee that these will be read after normal messages ?
        self._rabbit.route(dist_mean_queue, "control", build_eof_out_queue_name("dist_mean_provider"), self.__handle_type("dist_mean_eof"))
        self._rabbit.route(trip_count_queue, "control", build_eof_out_queue_name("trip_count_provider"), self.__handle_type("trip_count_eof"))
        self._rabbit.route(avg_queue, "control", build_eof_out_queue_name("avg_provider"), self.__handle_type("avg_eof"))
        
        self._rabbit.start()

    def start(self):
        thread = threading.Thread(target=self.__start)
        thread.start()
        thread.join()


def main():
    initialize_log(logging.INFO)
    response_provider = ResponseProvider(int(REPLICA_ID))
    response_provider.start()


if __name__ == "__main__":
    main()
