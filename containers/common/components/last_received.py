import pickle

from common.packets.generic_packet import GenericPacket
from common.utils import min_hash, log_duplicate, trace


class MultiLastReceivedManager:
    def __init__(self):
        self._last_received = {}

    def update(self, packet: GenericPacket) -> bool:
        sender_id = packet.sender_id

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

        trace(
            f"Received {sender_id}-{current_id}-{min_hash(packet.data)}")

        return True

    def get_state(self) -> bytes:
        return pickle.dumps(self._last_received)

    def set_state(self, state: bytes):
        self._last_received = pickle.loads(state)
