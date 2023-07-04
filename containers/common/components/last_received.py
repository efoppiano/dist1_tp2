import logging
from typing import Optional, List, Dict, Union

from common.packets.generic_packet import GenericPacket
from common.utils import min_hash, log_duplicate, trace


class MultiLastReceivedManager:
    def __init__(self):
        self._last_received: Dict[str, List[Optional[str]]] = {}
        self.maybe_dup = False

    def update(self, packet: GenericPacket) -> bool:

        sender_id = packet.sender_id

        current_id = packet.get_id()
        current_hash = packet.hash

        self._last_received.setdefault(sender_id, [None, None, None])  # [chunk_id, eof_id, hash]
        last_chunk_id, last_eof_id, last_hash = self._last_received[sender_id]

        if packet.is_eof():
            if current_id == last_eof_id:
                log_duplicate(
                    f"Received duplicate EOF {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                if not (packet.maybe_dup or self.maybe_dup):
                    logging.critical(
                        f"Received bad duplicate EOF {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                if current_hash != last_hash:
                    logging.critical(
                        f"Received different EOF hash {sender_id}-{current_id}-{last_hash} vs {current_hash}")
                self.maybe_dup = False
                return False
            self._last_received[sender_id][1] = current_id
        elif packet.is_chunk():
            if current_id == last_chunk_id:
                log_duplicate(
                    f"Received duplicate chunk {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                if not (packet.maybe_dup or self.maybe_dup):
                    logging.critical(
                        f"Received bad duplicate chunk {sender_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                if current_hash != last_hash:
                    logging.critical(
                        f"Received different chunk hash {sender_id}-{current_id}-{last_hash} vs {current_hash}")
                self.maybe_dup = False
                return False
            self._last_received[sender_id][0] = current_id

        self._last_received[sender_id][2] = current_hash

        logging.debug(
            f"Received {sender_id}-{current_id}-{min_hash(packet.data)}")
        self.maybe_dup = False
        return True

    def get_state(self) -> dict:
        return self._last_received

    def set_state(self, state: dict):
        self._last_received = state
