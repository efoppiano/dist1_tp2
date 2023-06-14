import os
import time
from common.packets.health_check import HealthCheck

HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")
CONTAINER_ID = os.environ["CONTAINER_ID"]
HEALTHCHECKER = os.environ["HEALTH_CHECKER"]
LAPSE = int(os.environ.get("HEARTBEAT_LAPSE", 1))

class HeartBeater:
    def __init__(self, _rabbit, container_id: str = CONTAINER_ID, healthchecker: str= HEALTHCHECKER, lapse: int= LAPSE ) -> None:
        self._rabbit = _rabbit
        self._container_id = container_id
        self._healthchecker = healthchecker
        self._lapse = lapse

    def start(self):
        self._rabbit.call_later(self._lapse, self._send_heartbeat)

    def _send_heartbeat(self):

        packet = HealthCheck(
            self._container_id,
            time.time_ns()
        ).encode()

        self._rabbit.send_to_route(HEARTBEAT_EXCHANGE, self._healthchecker, packet)
        self._rabbit.call_later(self._lapse, self._send_heartbeat)