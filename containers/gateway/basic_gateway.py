import abc
import logging
import math
import os
import pickle
import time
from abc import ABC
from typing import Dict, List

from common.components.heartbeater import HeartBeater
from common.components.message_sender import MessageSender
from common.components.readers import ClientIdResponsePacket
from common.packets.client_control_packet import ClientControlPacket, RateLimitChangeRequest
from common.packets.client_packet import ClientDataPacket, ClientPacket
from common.rate_checker import RateChecker, Rates
from common.router import Router
from common.utils import save_state, load_state, min_hash, log_duplicate, trace
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacketBuilder
from common.packets.gateway_or_static import is_side_table_message
from common.middleware.rabbit_middleware import Rabbit
from client_healthcheck import ClientHealthChecker

CONTAINER_ID = os.environ["CONTAINER_ID"]

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
EOF_ROUTING_KEY = os.environ["EOF_ROUTING_KEY"]

NEXT = os.environ["NEXT"]
NEXT_AMOUNT = int(os.environ["NEXT_AMOUNT"])

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
MAX_SEQ_NUMBER = 2 ** 10  # 2 packet ids would be enough, but we use more for traceability

RATE_CHECK_INTERVAL = 5


class BasicGateway(ABC):
    def __init__(self, container_id: str = CONTAINER_ID):
        self.__setup_middleware()

        self._basic_gateway_container_id = container_id
        self._last_chunk_received = None
        self._last_eof_received = None

        self.router = Router(NEXT, NEXT_AMOUNT)
        self.heartbeater = HeartBeater(self._rabbit)
        self._rate_checker = RateChecker()
        self._message_sender = MessageSender(self._rabbit)
        self.health_checker = ClientHealthChecker(
            self._rabbit, self.router, self._basic_gateway_container_id, self.save_state)

        self.__setup_state()

    def __setup_state(self):
        state = load_state()
        if state is not None:
            self.set_state(state)

    def __setup_middleware(self):
        self._rabbit = Rabbit(RABBIT_HOST)
        self._input_queue = INPUT_QUEUE
        self._rabbit.consume(self._input_queue, self.__on_stream_message_callback)
        eof_routing_key = EOF_ROUTING_KEY
        logging.info(f"Routing packets to {self._input_queue} using routing key {eof_routing_key}")
        self._rabbit.route(self._input_queue, "publish", eof_routing_key)

    def __handle_chunk(self, flow_id, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(flow_id, message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def __on_stream_message_without_duplicates(self, decoded: ClientDataPacket) -> bool:
        flow_id = decoded.get_flow_id()
        is_eof = decoded.is_eof()
        is_side_table = False

        self.health_checker.ping(decoded.client_id, decoded.city_name, is_eof)

        if is_eof:
            outgoing_messages = self.handle_eof(decoded.data)
        elif decoded.is_chunk():
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
            is_side_table = is_side_table_message(decoded.data[0])
        else:
            raise Exception(f"Unknown message type: {type(decoded.data)}")

        builder = GenericPacketBuilder(self._basic_gateway_container_id, decoded.client_id, decoded.city_name)
        self._message_sender.send(builder, outgoing_messages, skip_packet_id = is_side_table)

        return True

    def __update_last_received(self, packet: ClientDataPacket) -> bool:
        packet_id = packet.get_id()

        if packet.is_eof():
            if packet_id == self._last_eof_received:
                log_duplicate(
                    f"Received duplicate EOF {packet_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_eof_received = packet_id
        elif packet.is_chunk():
            if packet_id == self._last_chunk_received:
                log_duplicate(
                    f"Received duplicate chunk {packet_id}-{min_hash(packet.data)} - ignoring")
                return False
            self._last_chunk_received = packet_id

        trace(f"Received {packet_id}-{min_hash(packet.data)}")

        return True

    def __generate_and_send_client_id(self):
        new_client_id = f"{self._basic_gateway_container_id}_{time.time_ns()}"
        logging.info(f"New client id: {new_client_id}")

        control_queue = f"control_{new_client_id}"
        results_queue = f"results_{new_client_id}"
        self._rabbit.declare_queue(control_queue)
        self._rabbit.declare_queue(results_queue)

        response = ClientIdResponsePacket(new_client_id).encode()

        self._rabbit.produce("client_id_queue", response)
        self.health_checker.ping(new_client_id, None, False)

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = ClientPacket.decode(msg)
        if not isinstance(decoded.data, ClientDataPacket):
            self.__generate_and_send_client_id()
            return True

        if not self.health_checker.is_client(decoded.data.client_id):
            # Got data from a client already marked as dead, it should realize its
            # session is expired from the control message or the closing of its queues
            logging.debug(f"Received data from dead client {decoded.data.client_id}")
            return True

        if not self.__update_last_received(decoded.data):
            return True

        if not self.__on_stream_message_without_duplicates(decoded.data):
            return False

        self.save_state()
        return True

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    def handle_eof(self, message: Eof) -> Dict[str, Eof]:
        eof_output_queue = self.router.publish()

        return {
            eof_output_queue: message
        }

    def __change_rates_if_needed(self, rates: Rates):
        users_amount = len(self.health_checker.get_clients())
        trace("rates: %d ack/s %d pub*s | users_amount: %d | difference %d",
              rates.ack_per_second, rates.publish_per_second,
              users_amount, rates.ack_per_second - rates.publish_per_second)
        if users_amount == 0:
            return

        # TODO: Make sure not to overload the system
        if rates.ack_per_second - rates.publish_per_second >= 0.0:
            # Increase rate
            new_rate = math.ceil((2 * (rates.publish_per_second + 1)) / users_amount)
        else:
            # Decrease rate
            new_rate = math.ceil((rates.ack_per_second + 1) / users_amount)

        trace(f"action: change_rate | new_rate: {new_rate}")

        if new_rate == 0:
            trace("Rate not changed because it would be 0")
            return

        for user in self.health_checker.get_clients():
            client_control_queue = f"control_{user}"
            packet = RateLimitChangeRequest(new_rate)

            self._rabbit.produce(client_control_queue, ClientControlPacket(packet).encode())

        self.health_checker.set_expected_client_rate(new_rate)

    def __check_rates(self):
        rates = self._rate_checker.get_rates(self._input_queue)
        if rates is None:
            logging.debug("action: check_rate | result: no data")
        else:
            self.__change_rates_if_needed(rates)

        self._rabbit.call_later(RATE_CHECK_INTERVAL, self.__check_rates)

    def start(self):
        self.heartbeater.start()
        self._rabbit.call_later(RATE_CHECK_INTERVAL, self.__check_rates)
        self.health_checker.start()
        self._rabbit.start()

    def get_state(self) -> bytes:
        state = {
            "message_sender": self._message_sender.get_state(),
            "last_chunk_received": self._last_chunk_received,
            "last_eof_received": self._last_eof_received,
            "health_checker": self.health_checker.get_state(),
        }
        return pickle.dumps(state)

    def set_state(self, state: bytes):
        state = pickle.loads(state)
        self._message_sender.set_state(state["message_sender"])
        self.health_checker.set_state(state["health_checker"])
        self._last_chunk_received = state["last_chunk_received"]
        self._last_eof_received = state["last_eof_received"]

    def save_state(self):
        save_state(self.get_state())
