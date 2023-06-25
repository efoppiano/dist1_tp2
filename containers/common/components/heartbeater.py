import os
import time
from common.packets.health_check import HealthCheck

HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")
CONTAINER_ID = os.environ["CONTAINER_ID"]
HEALTHCHECKER = os.environ["HEALTH_CHECKER"]
LAPSE = int(os.environ.get("HEARTBEAT_LAPSE", 1))
INITIAL_GRACE_FACTOR = 10


class HeartBeater:
    def __init__(self, _rabbit, container_id: str = CONTAINER_ID, healthchecker: str = HEALTHCHECKER,
                 lapse: int = LAPSE) -> None:
        self._rabbit = _rabbit
        self._container_id = container_id
        self._healthchecker = healthchecker
        self._lapse = lapse
        self._last_heartbeat = time.time_ns()
        self._max_lapse = None

    def start(self):
        self._rabbit.call_later(self._lapse, self._send_heartbeat)

    def _send_heartbeat(self):

        lapse = time.time_ns() - self._last_heartbeat
        self._last_heartbeat = time.time_ns()

        if self._max_lapse is None:
            self._max_lapse = lapse
            max_lapse = self._lapse* INITIAL_GRACE_FACTOR
        else:
            if lapse > self._max_lapse:
                self._max_lapse = lapse
            max_lapse = self._max_lapse

        packet = HealthCheck(
            self._container_id,
            time.time_ns(), 
            max_lapse
        ).encode()

        self._rabbit.send_to_route(HEARTBEAT_EXCHANGE, self._healthchecker, packet)
        self._rabbit.call_later(self._lapse, self._send_heartbeat)
