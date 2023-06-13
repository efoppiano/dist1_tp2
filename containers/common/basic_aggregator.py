import abc
import logging
import os
import pickle
from abc import ABC
from typing import Dict, List

from common.last_received import MultiLastReceivedManager
from common.message_sender import MessageSender
from common.router import MultiRouter
from common.utils import save_state, load_state
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket, GenericPacketBuilder
from common.rabbit_middleware import Rabbit

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
PREV_AMOUNT = int(os.environ["PREV_AMOUNT"])
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
MAX_PACKET_ID = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability


class BasicAggregator(ABC):
    def __init__(self, router: MultiRouter, replica_id: int, side_table_routing_key: str):
        self.__setup_middleware(side_table_routing_key)

        self._basic_agg_replica_id = replica_id
        self._last_received = MultiLastReceivedManager()
        self._message_sender = MessageSender(self._rabbit)
        self._eofs_received = {}

        self.router = router

        self.__setup_state()

    def __setup_state(self):
        state = load_state()
        if state is not None:
            self.set_state(state)

    def __setup_middleware(self, side_table_routing_key: str):
        self._rabbit = Rabbit(RABBIT_HOST)
        input_queue = INPUT_QUEUE
        self._rabbit.consume(input_queue, self.__on_stream_message_callback)
        eof_routing_key = EOF_ROUTING_KEY
        logging.info(f"Routing packets to {input_queue} using routing key {eof_routing_key}")
        self._rabbit.route(input_queue, "publish", eof_routing_key)

        self._rabbit.route(input_queue, "publish", side_table_routing_key)

    def __handle_chunk(self, flow_id, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(flow_id, message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def __on_stream_message_without_duplicates(self, decoded: GenericPacket) -> bool:
        flow_id = decoded.get_flow_id()

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof_message(flow_id, decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise Exception(f"Unknown message type: {type(decoded.data)}")

        builder = GenericPacketBuilder(self._basic_agg_replica_id, decoded.client_id, decoded.city_name)
        self._message_sender.send(builder, outgoing_messages)

        return True

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)

        if not self._last_received.update(decoded):
            return True

        if not self.__on_stream_message_without_duplicates(decoded):
            return False

        save_state(self.get_state())
        return True

    def handle_eof_message(self, flow_id, message: Eof) -> Dict[str, Eof]:
        self._eofs_received.setdefault(flow_id, 0)
        self._eofs_received[flow_id] += 1

        if self._eofs_received[flow_id] < PREV_AMOUNT:
            return {}

        self._eofs_received.pop(flow_id)

        return self.handle_eof(flow_id, message)

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    def handle_eof(self, flow_id, message: Eof) -> Dict[str, Eof]:
        eof_output_queues = self.router.publish()
        output = {}
        for queue in eof_output_queues:
            output[queue] = message

        return output

    @abc.abstractmethod
    def get_state(self) -> bytes:
        state = {
            "message_sender": self._message_sender.get_state(),
        }
        return pickle.dumps(state)

    @abc.abstractmethod
    def set_state(self, state_bytes: bytes):
        state = pickle.loads(state_bytes)
        self._message_sender.set_state(state["message_sender"])

    def start(self):
        self._rabbit.start()
