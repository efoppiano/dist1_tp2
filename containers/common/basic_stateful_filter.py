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
        self._last_hash_by_replica = {}
        self._eofs_received = set()

        state = self.__load_full_state()
        if state is not None:
            logging.info(f"Found previous state, setting it")
            self.__set_full_state(state)

    @abc.abstractmethod
    def get_state(self) -> bytes:
        pass

    @abc.abstractmethod
    def set_state(self, state: bytes):
        pass

    def on_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)
        replica_id = decoded.replica_id
        if isinstance(decoded.data, Eof):
            if decoded.data.city_name in self._eofs_received:
                logging.info(f"Received duplicate EOF from city {decoded.data.city_name} - ignoring")
                return True
            self._eofs_received.add(decoded.data.city_name)
        else:
            msg_hash = utils.hash_msg(msg)
            if msg_hash == self._last_hash_by_replica.get(replica_id):
                logging.info(f"Received duplicate message from replica {replica_id} - ignoring")
                return True
            self._last_hash_by_replica[replica_id] = msg_hash

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
        self._last_hash_by_replica = state["last_hash_by_replica"]
        self._eofs_received = state["eofs_received"]

    def __save_full_state(self):
        state = {
            "concrete_state": self.get_state(),
            "last_hash_by_replica": self._last_hash_by_replica,
            "eofs_received": self._eofs_received
        }
        utils.save_state(pickle.dumps(state))
