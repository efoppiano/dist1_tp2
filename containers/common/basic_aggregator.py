import abc
import logging
import os
import pickle
from abc import ABC
from typing import Dict, List, Union

from common import utils
from common.linker.linker import Linker
from common.packets.eof import Eof
from common.packets.generic_packet import GenericPacket, PacketIdentifier
from common.rabbit_middleware import Rabbit

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")


class BasicAggregator(ABC):
    def __init__(self, replica_id: int, side_table_routing_key: str):
        self._basic_agg_replica_id = replica_id

        self._rabbit = Rabbit(RABBIT_HOST)
        input_queue = Linker().get_input_queue(self, replica_id)
        self._rabbit.consume(input_queue, self.__on_stream_message_callback)
        eof_routing_key = Linker().get_eof_out_routing_key(self)
        logging.info(f"Routing packets to {input_queue} using routing key {eof_routing_key}")
        self._rabbit.route(input_queue, "control", eof_routing_key)

        self._rabbit.route(input_queue, "publish", side_table_routing_key)

        self._last_received = {}
        self._eofs_received = set()

        state = self.__load_full_state()
        if state is not None:
            self.__set_full_state(state)

    def __handle_chunk(self, flow_id, chunk: List[bytes]) -> Dict[str, List[bytes]]:
        outgoing_messages = {}
        for message in chunk:
            responses = self.handle_message(flow_id, message)
            for (queue, messages) in responses.items():
                outgoing_messages.setdefault(queue, [])
                outgoing_messages[queue] += messages
        return outgoing_messages

    def __send_messages(self, id: PacketIdentifier, outgoing_messages: Dict[str, List[bytes]]):
        for (queue, messages) in outgoing_messages.items():
            if queue.endswith("_eof_in"):
                for message in messages:
                    self._rabbit.produce(queue, message)
            elif len(messages) > 0:
                encoded = GenericPacket(
                    replica_id=self._basic_agg_replica_id,
                    client_id=id.client_id,
                    city_name=id.city_name,
                    packet_id=id.packet_id,
                    data=messages
                ).encode()
                self._rabbit.produce(queue, encoded)

    def __on_stream_message_without_duplicates(self, id: PacketIdentifier, decoded: GenericPacket) -> bool:
        
        flow_id = ( decoded.client_id, decoded.city_name )

        if isinstance(decoded.data, Eof):
            outgoing_messages = self.handle_eof(flow_id, decoded.data)
        elif isinstance(decoded.data, bytes):
            outgoing_messages = self.handle_message(flow_id, decoded.data)
        elif isinstance(decoded.data, list):
            outgoing_messages = self.__handle_chunk(flow_id, decoded.data)
        else:
            raise Exception(f"Unknown message type: {type(decoded.data)}")

        self.__send_messages(id, outgoing_messages)

        return True

    def __update_last_received(self, packet: GenericPacket):
        
        replica_id = packet.replica_id
        flow_id = ( packet.client_id, packet.city_name )
        packet_id = packet.packet_id

        if isinstance(packet.data, Eof):
            if flow_id in self._eofs_received:
                logging.info(f"Received duplicate EOF from city {id} - ignoring")
                return False
            self._eofs_received.add(flow_id)
        else:
            # self.last_received [flow_id][replica_id] = packet_id
            self._last_received.setdefault(flow_id, {})
            self._last_received[flow_id].setdefault(replica_id, -1)
            if packet_id == self._last_received[flow_id][replica_id]:
                logging.info(f"Received duplicate message from replica {replica_id} - ignoring")
                return False
            self._last_received[flow_id][replica_id] = packet_id

        return True

    def __on_stream_message_callback(self, msg: bytes) -> bool:
        decoded = GenericPacket.decode(msg)
        
        if not self.__update_last_received(decoded):
            return True

        id = PacketIdentifier(
            self._basic_agg_replica_id,
            decoded.client_id,
            decoded.city_name,
            decoded.packet_id
        )
        if not self.__on_stream_message_without_duplicates(id, decoded):
            return False

        self.__save_full_state()
        return True

    @abc.abstractmethod
    def handle_message(self, flow_id, message: bytes) -> Dict[str, List[bytes]]:
        pass

    @abc.abstractmethod
    def handle_eof(self, flow_id, message: Eof) -> Dict[str, List[bytes]]:
        pass

    def start(self):
        self._rabbit.start()

    @abc.abstractmethod
    def get_state(self) -> bytes:
        pass

    @abc.abstractmethod
    def set_state(self, state: bytes):
        pass

    def __save_full_state(self):
        state = {
            "concrete_state": self.get_state(),
            "_last_received": self._last_received,
            "_eofs_received": self._eofs_received,
        }
        utils.save_state(pickle.dumps(state))

    def __load_full_state(self) -> Union[dict, None]:
        state = utils.load_state()
        if not state:
            return None
        return pickle.loads(state)

    def __set_full_state(self, state: dict):
        self.set_state(state["concrete_state"])
        self._last_received = state["_last_received"]
        self._eofs_received = state["_eofs_received"]
