import socket
import logging
import os
import time
from abc import ABC, abstractmethod

from typing import List

from common.utils import trace
from common.packets.health_check import HealthCheck
from common.components.invoker import Invoker

CONTAINER_ID = os.environ["CONTAINER_ID"]
HEALTH_CHECK_PORT = os.environ.get("HEALTH_CHECK_PORT", 5005)
CONTAINERS = os.environ["CONTAINERS"].split(",")

HEALTH_CHECK_TIMEOUT_SEC = float(os.environ.get("HEALTH_CHECK_TIMEOUT_SEC", 0.5))
HEALTH_CHECK_INTERVAL_SEC = int(os.environ.get("HEALTH_CHECK_INTERVAL_SEC", 2))
GRACE_INTERVALS = int(os.environ.get("GRACE_INTERVALS", 5))

HEALTHCHECKER = os.environ["HEALTH_CHECKER"]
HEARTBEAT_LAPSE = int(os.environ.get("HEARTBEAT_LAPSE", 1))

S_TO_NS = 10 ** 9
START_TIME = time.time_ns()


class BasicHealthChecker(ABC):
    def __init__(self, ids_to_monitor: List[str] = CONTAINERS):
        self._ids_to_monitor = ids_to_monitor
        self._last_seen = {id_to_monitor: START_TIME for id_to_monitor in ids_to_monitor}
        self._last_check = time.time()

        self._container_id = CONTAINER_ID
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.check_invoker = Invoker(HEALTH_CHECK_INTERVAL_SEC, self.__check_health)

        self._healthchecker = HEALTHCHECKER
        self.hearbeat_invoker = Invoker(HEARTBEAT_LAPSE, self._send_heartbeat)

    def _send_heartbeat(self):
        packet = HealthCheck(
            self._container_id,
            time.time_ns()
        ).encode()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(packet, (self._healthchecker, HEALTH_CHECK_PORT))
        except:
            logging.error(f"Failed to send heartbeat to {self._healthchecker}:{HEALTH_CHECK_PORT}")

    def on_message_callback(self, msg: bytes) -> bool:
        packet = HealthCheck.decode(msg)
        difference = (time.time_ns() - packet.timestamp) / S_TO_NS
        trace(f"Received health check from {packet.id} with {difference} seconds of difference from now")
        if packet.id in self._ids_to_monitor:
            self._last_seen[packet.id] = packet.timestamp

        return True

    @abstractmethod
    def on_check_fail(self, failed_id: str):
        pass

    def __check_health(self):
        start_time = time.time()
        for (id_to_monitor, last_seen) in self._last_seen.items():
            if last_seen + S_TO_NS * HEALTH_CHECK_INTERVAL_SEC * GRACE_INTERVALS < time.time_ns():
                difference = (time.time_ns() - last_seen) / S_TO_NS
                logging.warning(f"Container {id_to_monitor} is unheard for {difference} seconds")
                self.on_check_fail(id_to_monitor)
                self._last_seen[id_to_monitor] = time.time_ns()
        
        end_time = time.time()
        logging.debug(f"Health check took {end_time - start_time} seconds - after {end_time - self._last_check} seconds")
        self._last_check = end_time

    def recv_msg(self):
        try:
            packet_length = int.from_bytes(self.socket.recv(2), "big")
            data, addr = self.socket.recvfrom(packet_length)
            self.on_message_callback(data)
        except socket.timeout:
            pass

    def main_loop(self, msg_timeout: float = HEALTH_CHECK_TIMEOUT_SEC):
        self.socket.bind(("", HEALTH_CHECK_PORT))
        self.socket.settimeout(msg_timeout)

        while True:
            self.recv_msg()
            self.hearbeat_invoker.check()
            self.check_invoker.check()

        

    def start(self):
        self.main_loop()
