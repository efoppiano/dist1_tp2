import abc
import logging
import os
import pickle
from abc import ABC
from typing import Optional

from common import utils
from common.basic_filter import BasicFilter
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")


class BasicStatefulFilter(BasicFilter, ABC):
    def __init__(self, replica_id: int):
        super().__init__(replica_id)
        self._last_received = {}
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
        flow_id = ( packet.client_id, packet.city_name )
        packet_id = packet.packet_id

        if flow_id == (None, None): return True

        if isinstance(packet.data, Eof):
            if flow_id in self._eofs_received:
                logging.warning(f"Received duplicate EOF from flow_id {flow_id} - ignoring")
                return False
            self._eofs_received.add(flow_id)
        else:
            # self.last_received [flow_id][replica_id] = packet_id
            self._last_received.setdefault(flow_id, {})
            last_packet_id = self._last_received[flow_id].get(replica_id)
            if packet_id == last_packet_id and packet_id is not None:
                logging.warning(f"Received duplicate message for {flow_id} from replica {replica_id} - ignoring")
                return False
            self._last_received[flow_id][replica_id] = packet_id

        return True

    def on_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)
        
        if not self.__update_last_received(decoded):
            return True

        if not super().on_message_callback(decoded):
            return False

        self.__save_full_state()
        return True

    @staticmethod
    def __load_full_state() -> Optional[dict]:
        state = utils.load_state()
        if not state:
            return None
        return pickle.loads(state)

    def __set_full_state(self, state: dict):
        self.set_state(state["concrete_state"])
        self._last_received = state["_last_received"]
        self._eofs_received = state["_eofs_received"]

    def __save_full_state(self):
        state = {
            "concrete_state": self.get_state(),
            "_last_received": self._last_received,
            "_eofs_received": self._eofs_received
        }
        utils.save_state(pickle.dumps(state))
