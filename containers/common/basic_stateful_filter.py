import abc
import logging
import os
import pickle
from abc import ABC
from typing import Dict, List, Optional

from common.utils import save_state, load_state, min_hash
from common.basic_filter import BasicFilter
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket
from common.packets.generic_packet import PacketIdentifier

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
MAX_PACKET_ID = 2**10 # 2 packet ids would be enough, but we more for traceability

class BasicStatefulFilter(BasicFilter, ABC):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)
        self._last_received = {}
        self._last_packet_id = 0
        self._eofs_received = set()

        state = self.__load_full_state()
        if state is not None:
            logging.info(f"Found previous state, setting it")
            self.__set_full_state(state)

    def get_state(self) -> bytes:
        return b""

    def set_state(self, state: bytes):
        pass
    
    def __update_last_received(self, packet: GenericPacket):
        
        replica_id = packet.replica_id

        if isinstance(packet.data, Eof):
            flow_id = ( packet.client_id, packet.city_name )
            if flow_id in self._eofs_received:
                logging.warning(f"Received duplicate EOF from flow_id {flow_id} - ignoring")
                return False
            self._eofs_received.add(flow_id)
        else:
            current_id = packet.packet_id
            last_id = self._last_received.get(replica_id)
            if current_id == last_id:
                logging.warning(f"Received duplicate {replica_id}-{current_id}-{min_hash(packet.data)} - ignoring")
                return False
            logging.debug(f"Received {replica_id}-{current_id}-{min_hash(packet.data)}")
            self._last_received[replica_id] = current_id

        return True

    def on_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)
        
        if not self.__update_last_received(decoded):
            return True

        if not super().on_message_callback(decoded):
            return False

        self.__save_full_state()
        return True

    def __next_packet_id(self, _id: PacketIdentifier) -> int:
        self._last_packet_id = (self._last_packet_id + 1) % MAX_PACKET_ID
        return self._last_packet_id

    def send_messages(self, id: PacketIdentifier, outgoing_messages: Dict[str, List[bytes]]):
        id.replica_id = self._basic_filter_replica_id
        id.packet_id = self.__next_packet_id(id)
        return super().send_messages(id, outgoing_messages)
    
    @staticmethod
    def __load_full_state() -> Optional[dict]:
        state = load_state()
        if not state:
            return None
        return pickle.loads(state)

    def __set_full_state(self, state: dict):
        self.set_state(state["concrete_state"])
        self._last_received = state["_last_received"]
        self._last_packet_id = state["_last_packet_id"]
        self._eofs_received = state["_eofs_received"]

    def __save_full_state(self):
        state = {
            "concrete_state": self.get_state(),
            "_last_received": self._last_received,
            "_last_packet_id": self._last_packet_id,
            "_eofs_received": self._eofs_received
        }
        save_state(pickle.dumps(state))
