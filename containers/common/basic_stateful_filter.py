import os
import pickle
from abc import ABC
from typing import Dict, List, Optional, Union

from common.last_received import MultiLastReceivedManager
from common.utils import save_state, load_state
from common.router import Router
from common.basic_filter import BasicFilter
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
PREV_AMOUNT = int(os.environ["PREV_AMOUNT"])
NEXT = os.environ["NEXT"]
NEXT_AMOUNT = os.environ.get("NEXT_AMOUNT")
if NEXT_AMOUNT is not None:
    NEXT_AMOUNT = int(NEXT_AMOUNT)
MAX_SEQ_NUMBER = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability


class BasicStatefulFilter(BasicFilter, ABC):
    def __init__(self, replica_id: int):
        self._last_received = MultiLastReceivedManager()
        self._eofs_received = {}

        self.router = Router(NEXT, NEXT_AMOUNT)
        super().__init__(replica_id)

    def get_state(self) -> bytes:
        return pickle.dumps({
            "last_received": self._last_received,
            "eofs_received": self._eofs_received,
            "parent_state": super().get_state()
        })

    def set_state(self, state: dict):
        self._last_received = state["last_received"]
        self._eofs_received = state["eofs_received"]
        super().set_state(state["parent_state"])

    def on_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)

        if not self._last_received.update(decoded):
            return True

        if not super().on_message_callback(decoded):
            return False

        self.__save_full_state()
        return True

    def handle_eof(self, flow_id, message: Eof) -> Dict[str, Union[List[bytes], Eof]]:
        eof_output_queue = self.router.publish()
        return {
            eof_output_queue: message
        }

    def handle_eof_message(self, flow_id, message: Eof) -> Dict[str, Union[List[bytes], Eof]]:
        self._eofs_received.setdefault(flow_id, 0)
        self._eofs_received[flow_id] += 1

        if self._eofs_received[flow_id] < PREV_AMOUNT:
            return {}

        self._eofs_received.pop(flow_id)

        return self.handle_eof(flow_id, message)

    @staticmethod
    def __load_full_state() -> Optional[dict]:
        state = load_state()
        if not state:
            return None
        return pickle.loads(state)

    def __set_full_state(self, state: dict):
        self.set_state(state["concrete_state"])
        self._last_received.set_state(state["_last_received"])
        self._last_seq_number = state["_last_seq_number"]
        self._eofs_received = state["_eofs_received"]

    def __save_full_state(self):
        state = {
            "concrete_state": self.get_state(),
            "_last_received": self._last_received.get_state(),
            "_last_seq_number": self._last_seq_number,
            "_eofs_received": self._eofs_received
        }
        save_state(pickle.dumps(state))
