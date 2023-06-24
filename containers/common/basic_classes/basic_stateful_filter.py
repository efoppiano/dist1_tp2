import logging
import os
import pickle
from abc import ABC
from typing import Dict, List, Union

from common.components.last_received import MultiLastReceivedManager
from common.router import Router
from common.basic_classes.basic_filter import BasicFilter
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket

CONTAINER_ID = os.environ["CONTAINER_ID"]
RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
PREV_AMOUNT = int(os.environ["PREV_AMOUNT"])
NEXT = os.environ["NEXT"]
NEXT_AMOUNT = os.environ.get("NEXT_AMOUNT")
if NEXT_AMOUNT is not None:
    NEXT_AMOUNT = int(NEXT_AMOUNT)
MAX_SEQ_NUMBER = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability


class BasicStatefulFilter(BasicFilter, ABC):
    def __init__(self, container_id: str = CONTAINER_ID):
        self._last_received = MultiLastReceivedManager()
        self._eofs_received = {}

        self.router = Router(NEXT, NEXT_AMOUNT)
        super().__init__(container_id)

    def get_state(self) -> bytes:
        return pickle.dumps({
            "last_received": self._last_received.get_state(),
            "eofs_received": self._eofs_received,
            "parent_state": super().get_state()
        })

    def set_state(self, state_bytes: bytes):
        state = pickle.loads(state_bytes)
        self._last_received.set_state(state["last_received"])
        self._eofs_received = state["eofs_received"]
        super().set_state(state["parent_state"])

    def on_message_callback(self, msg: bytes) -> bool:
        logging.debug(f"Received message of size {len(msg)}")
        decoded = GenericPacket.decode(msg)

        if not self._last_received.update(decoded):
            return True

        if not super().on_message_callback(decoded):
            return False

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
            logging.debug(f"Received EOF for flow {flow_id} ({self._eofs_received[flow_id]}/{PREV_AMOUNT})")
            return {}
        logging.info(f"Received EOF for flow {flow_id} ({self._eofs_received[flow_id]}/{PREV_AMOUNT})")

        self._eofs_received.pop(flow_id)

        return self.handle_eof(flow_id, message)
