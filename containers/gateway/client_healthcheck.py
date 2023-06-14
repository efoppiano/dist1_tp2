import time
from common.router import Router
from common.packets.eof import Eof
from common.middleware.rabbit_middleware import Rabbit
from common.components.message_sender import MessageSender
from common.packets.generic_packet import GenericPacketBuilder


HEALTHCHECK_LAPSE = 10
CLIENT_TIMEOUT = 10
EVICTION_TIME = 10

class ClientHealthChecker:

    def __init__(self,
                 _rabbit: Rabbit,
                 router: Router,
                 message_sender: MessageSender,
                 replica_id: int,
                 save_state: function,
                 lapse: int = HEALTHCHECK_LAPSE,
                 client_timeout: int = CLIENT_TIMEOUT,
                 eviction_time: int = EVICTION_TIME
                ) -> None:
        
        self._rabbit = _rabbit
        self._output_queue = router.publish()
        self._message_sender = message_sender
        self._replica_id = replica_id
        self._save_state = save_state

        self._lapse = lapse
        self._client_timeout = client_timeout
        self._eviction_time = eviction_time

        self._clients = {} # [client_id]: (last_city, last_time, finished)

    def evict(self, client_id: str, last_city: str = None, finished: bool = False):
        builder = GenericPacketBuilder(self._replica_id, client_id, last_city)

        eof = Eof(finished, self._eviction_time)
        outgoing_messages = { self._output_queue: eof }

        self._message_sender.send(builder, outgoing_messages)
        del self._clients[client_id]

    def check_clients(self):
        now = time.time()
        for client_id, (last_city, last_time, finished) in self._clients.items():
            if now - last_time > self._client_timeout:
                self.evict(client_id, last_city, finished)

        self._rabbit.call_later(self._lapse, self.check_clients)
        self._save_state()

    def start(self):
        self._rabbit.call_later(self._lapse, self.check_clients)

    def ping(self, client_id: str, city: str, finished: bool = False):
        self._clients[client_id] = (city, time.time(), finished)

    def get_clients(self):
        return self._clients.keys()

    def get_state(self) -> dict:
        return self._clients
    
    def set_state(self, state: dict):
        self._clients = state
