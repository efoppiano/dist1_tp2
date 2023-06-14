import logging
import pickle

from common.packets.generic_packet import GenericPacket
from common.utils import min_hash


class MultiLastReceivedManager:
    def __init__(self):
        self._last_received = {}

    def update(self, packet: GenericPacket) -> bool:
        replica_id = packet.replica_id

        current_id = packet.get_id()
        self._last_received.setdefault(replica_id, [None, None])
        last_chunk_id, last_eof_id = self._last_received[replica_id]

        if packet.is_eof():
            if current_id == last_eof_id:
                logging.warning(
                    f"Received duplicate EOF {replica_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_received[replica_id][1] = current_id
        elif packet.is_chunk():
            if current_id == last_chunk_id:
                logging.warning(
                    f"Received duplicate chunk {replica_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_received[replica_id][0] = current_id

        logging.debug(
            f"Received {replica_id}-{current_id}-{min_hash(packet.data)}")

        return True

    def get_state(self) -> bytes:
        return pickle.dumps(self._last_received)

    def set_state(self, state: bytes):
        self._last_received = pickle.loads(state)
