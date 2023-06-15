import os
import time
import socket
import logging
from common.packets.health_check import HealthCheck

HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")
CONTAINER_ID = os.environ["CONTAINER_ID"]
HEALTHCHECKER = os.environ["HEALTH_CHECKER"]
HEALTH_CHECK_PORT = os.environ.get("HEALTH_CHECK_PORT", 5005)
LAPSE = int(os.environ.get("HEARTBEAT_LAPSE", 1))


class HeartBeater:
    def __init__(self, _rabbit, container_id: str = CONTAINER_ID, healthchecker: str = HEALTHCHECKER,
                 lapse: int = LAPSE) -> None:
        self._rabbit = _rabbit
        self._container_id = container_id
        self._healthchecker = healthchecker
        self._lapse = lapse

    def start(self):
        self._rabbit.call_later(self._lapse, self._send_heartbeat_loop)

    def _send_heartbeat_loop(self):
        self._send_heartbeat()
        self._rabbit.call_later(self._lapse, self._send_heartbeat_loop)

    def _send_heartbeat(self):
        packet = HealthCheck(
            self._container_id,
            time.time_ns()
        ).encode()

        packet_len = len(packet).to_bytes(2, byteorder="big")
        packet = packet_len + packet

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(packet, (self._healthchecker, HEALTH_CHECK_PORT))
        except:
            logging.error(f"Failed to send heartbeat to {self._healthchecker}:{HEALTH_CHECK_PORT}")
