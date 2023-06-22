import abc
import logging
import os
import pickle
from abc import ABC
from typing import Dict, List

from common.components.heartbeater import HeartBeater
from common.components.last_received import MultiLastReceivedManager
from common.components.message_sender import MessageSender
from common.router import MultiRouter
from common.utils import save_state, load_state
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket, GenericPacketBuilder
from common.middleware.rabbit_middleware import Rabbit
from common.packets.gatway_or_static import GatewayOrStatic
from common.packets.station_side_table_info import StationSideTableInfo
from common.packets.weather_side_table_info import WeatherSideTableInfo


SIDE_TABLE_ROUTING_KEY = os.environ["SIDE_TABLE_ROUTING_KEY"]
CONTAINER_ID = os.environ["CONTAINER_ID"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
PREV_AMOUNT = int(os.environ["PREV_AMOUNT"])
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
MAX_PACKET_ID = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability


def is_side_table_message(message: GenericPacket) -> bool:
    data = GatewayOrStatic.decode(message.data).data
    return isinstance(data, StationSideTableInfo) or isinstance(data, WeatherSideTableInfo)


class BasicAggregator(ABC):
    def __init__(self, router: MultiRouter, container_id: str = CONTAINER_ID,
                 side_table_routing_key: str = SIDE_TABLE_ROUTING_KEY):
        self.__setup_middleware(side_table_routing_key)

        self._basic_agg_container_id = container_id
        self._last_received = MultiLastReceivedManager()
        self._message_sender = MessageSender(self._rabbit)
        self._eofs_received = {}
        self.heartbeater = HeartBeater(self._rabbit)

        self.router = router

        self.__setup_state()

    def __setup_state(self):
        state = load_state("/volumes/state")
        if state is not None:
            self.set_state(state)
        inner_state = load_state("/volumes/inner_state")
        if inner_state is not None:
            self.__set_inner_state(inner_state)

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

        builder = GenericPacketBuilder(self._basic_agg_container_id, decoded.client_id, decoded.city_name)
        self._message_sender.send(builder, outgoing_messages)

        return True

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)
        side_table = is_side_table_message(decoded)

        # TODO: side table msgs should not update packet id
        if not side_table and not self._last_received.update(decoded):
            return True

        if not self.__on_stream_message_without_duplicates(decoded):
            return False

        if side_table:
            save_state(self.get_state(), "/volumes/state")
        else:
            save_state(self.__get_inner_state(), "/volumes/inner_state")
        return True

    def handle_eof_message(self, flow_id, message: Eof) -> Dict[str, Eof]:
        self._eofs_received.setdefault(flow_id, 0)
        self._eofs_received[flow_id] += 1

        logging.debug(f"Received EOF for flow {flow_id} ({self._eofs_received[flow_id]}/{PREV_AMOUNT})")
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
        return pickle.dumps({})

    @abc.abstractmethod
    def set_state(self, state_bytes: bytes):
        pass

    def __get_inner_state(self) -> bytes:
        state = {
            "message_sender": self._message_sender.get_state(),
            "last_received": self._last_received.get_state(),
            "eofs_received": self._eofs_received,
        }
        return pickle.dumps(state)
    
    def __set_inner_state(self, state_bytes: bytes):
        state = pickle.loads(state_bytes)
        self._message_sender.set_state(state["message_sender"])
        self._last_received.set_state(state["last_received"])
        self._eofs_received = state["eofs_received"]

    def start(self):
        self.heartbeater.start()
        self._rabbit.start()
