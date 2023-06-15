import time
from typing import Callable, Optional
from common.router import Router
from common.packets.eof import Eof
from common.middleware.rabbit_middleware import Rabbit
from common.components.message_sender import MessageSender
from common.packets.generic_packet import GenericPacketBuilder
from common.packets.client_control_packet import ClientControlPacket
from common.utils import log_evict

HEALTHCHECK_LAPSE = 10
INITIAL_CLIENT_TIMEOUT = 50
NEW_CLIENT_GRACE_FACTOR = 5
CLIENT_TIMEOUT_TO_LAPSE_RATIO = 10
EVICTION_TIME = 10


class ClientHealthChecker:

    def __init__(self,
                 _rabbit: Rabbit,
                 router: Router,
                 message_sender: MessageSender,
                 replica_id: int,
                 save_state: Callable,
                 lapse: int = HEALTHCHECK_LAPSE,
                 initial_client_timeout: float = INITIAL_CLIENT_TIMEOUT,
                 client_timeout_to_lapse_ratio: int = CLIENT_TIMEOUT_TO_LAPSE_RATIO,
                 eviction_time: int = EVICTION_TIME
                 ) -> None:

        self._rabbit = _rabbit
        self._output_queue = router.publish()
        self._message_sender = message_sender

        # TODO: Not sure if this could collide with the gateway, so I'm using a negative replica_id
        self._replica_id = -replica_id

        self._save_state = save_state
        self._lapse = lapse
        self._client_timeout = initial_client_timeout
        self._client_timeout_to_lapse_ratio = client_timeout_to_lapse_ratio
        self._eviction_time = eviction_time

        self._clients = {}  # [client_id]: (last_city, last_time, finished)
        self._evicting = set()  # [client_id]

    def evict(self, client_id: str, last_city: str = None, drop: bool = False):
        # Notify the client it has been evicted
        control_queue = f"control_{client_id}"
        self._rabbit.produce(control_queue, ClientControlPacket("SessionExpired").encode())

        # Send EOF to the next replica with eviction time
        builder = GenericPacketBuilder(self._replica_id, client_id, last_city)
        eof = Eof(drop, self._eviction_time)
        outgoing_messages = {self._output_queue: eof}

        self._message_sender.send(builder, outgoing_messages)
        if client_id in self._clients:
            del self._clients[client_id]

        log_evict(f"Evicting client {client_id} | Drop: {drop}")

    def check_clients(self):
        now = time.time()

        for client_id, (last_city, last_time, _) in self._clients.items():
            if last_city is None:
                timeout = INITIAL_CLIENT_TIMEOUT * NEW_CLIENT_GRACE_FACTOR
            else:
                timeout = self._client_timeout

            if now - last_time > timeout:
                self._evicting.add(client_id)
        self._save_state()

        while len(self._evicting) > 0:
            client_id = self._evicting.pop()
            last_city, _, finished = self._clients[client_id]
            self.evict(client_id, last_city, drop=not finished)
        self._save_state()

        self._rabbit.call_later(self._lapse, self.check_clients)

    def start(self):
        self._rabbit.call_later(self._lapse, self.check_clients)

    def ping(self, client_id: str, city: Optional[str], finished: bool = False):
        self._clients[client_id] = (city, time.time(), finished)

    def set_expected_client_rate(self, rate: float):
        expected_lapse = 1 / rate
        expected_timeout = expected_lapse * self._client_timeout_to_lapse_ratio

        # Timeout can increase sharply, but decreases slowly
        if expected_timeout > self._client_timeout:
            self._client_timeout = expected_timeout
        else:
            self._client_timeout = 0.8 * self._client_timeout + 0.2 * expected_timeout


    def get_clients(self):
        clients = set(self._clients.keys())

        for client_id in self._evicting:
            if client_id in clients:
                clients.remove(client_id)

        return clients

    def is_client(self, client_id: str):
        return client_id in self.get_clients()

    def get_state(self) -> dict:
        return {
            "clients": self._clients,
            "evicting": self._evicting
        }

    def set_state(self, state: dict):
        self._clients = state["clients"]
        self._evicting = state["evicting"]
