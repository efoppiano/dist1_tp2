import logging
import os
import signal
import pickle
import threading

from common.packets.generic_packet import GenericPacket
from common.packets.eof import Eof
from common.packets.client_response_packets import GenericResponsePacket
from common.rabbit_middleware import Rabbit
from common.utils import initialize_log, build_eof_out_queue_name, save_state, load_state

REPLICA_ID = os.environ["REPLICA_ID"]
SELF_QUEUE = f"sent_responses_{REPLICA_ID}"


class ResponseProvider:
    def __init__(self, replica_id: int):
        self._replica_id = replica_id

        self._last_hash_by_replica = {
            "dist_mean": {},
            "trip_count": {},
            "dur_avg": {},
            "dist_mean_eof": {},
            "trip_count_eof": {},
            "dur_avg_eof": {},
        }

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
        
        replica_id = packet.replica_id
        current_id = ( packet.client_id, packet.city_name, type, packet.packet_id )
        last_id = self._last_received.get(replica_id)

        if current_id == last_id:
            logging.warning(f"Received duplicate message from replica {replica_id}: {current_id} - ignoring")
            return False
        self._last_received[replica_id] = current_id

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
      save_state(pickle.dumps(self._last_hash_by_replica))
    
    def __load_state(self):
      try:
        _last_hash_by_replica = pickle.loads(load_state())
        if _last_hash_by_replica is not None:
          self._last_hash_by_replica = _last_hash_by_replica
      except:
        logging.warning("Failed to load state")
        pass

    def __load_last_sent(self):
      self._rabbit.consume_until_empty(SELF_QUEUE, self.__handle_last_sent)
    
    def __handle_last_sent(self, message: bytes) -> bool:
      packet = GenericResponsePacket.decode(message)
      flow_id = ( packet.client_id, packet.city_name )
      case_id = ( packet.type, packet.replica_id)
      
      self._last_hash_by_replica.setdefault(flow_id, {})
      self._last_hash_by_replica[flow_id][case_id] = packet.packet_id
      return True

    def __start(self):

        dist_mean_queue = f"dist_mean_provider_{REPLICA_ID}"
        trip_count_queue = f"trip_count_provider_{REPLICA_ID}"
        avg_queue = f"avg_provider_{REPLICA_ID}"

        self._rabbit.consume(dist_mean_queue, self.__handle_type("dist_mean"))
        self._rabbit.consume(trip_count_queue, self.__handle_type("trip_count"))
        self._rabbit.consume(avg_queue, self.__handle_type("dur_avg"))

        self._rabbit.route(dist_mean_queue, "control", build_eof_out_queue_name("dist_mean_provider"))
        self._rabbit.route(trip_count_queue, "control", build_eof_out_queue_name("trip_count_provider"))
        self._rabbit.route(avg_queue, "control", build_eof_out_queue_name("avg_provider"))
        
        # Returns True every time, as this is already saved to disk if reading at runtime
        self._rabbit.consume(SELF_QUEUE, lambda _message: True)

        self._rabbit.start()

    def start(self):
        thread = threading.Thread(target=self.__start)
        thread.start()
        thread.join()


def main():
    initialize_log()
    response_provider = ResponseProvider(int(REPLICA_ID))
    response_provider.start()


if __name__ == "__main__":
    main()
