import logging
import os
import time
from abc import ABC, abstractmethod

from typing import List

from common.heartbeater import HeartBeater
from common.packets.health_check import HealthCheck
from common.rabbit_middleware import Rabbit

HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")
CONTAINER_ID = os.environ["CONTAINER_ID"]
CONTAINERS = os.environ["CONTAINERS"].split(",")
RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
HEALTH_CHECK_INTERVAL_SEC = os.environ.get("HEALTH_CHECK_INTERVAL_SEC", 1)

S_TO_NS = 10 ** 9
START_TIME = time.time_ns()


class BasicHealthChecker(ABC):
    def __init__(self, ids_to_monitor: List[str] = CONTAINERS):
        self._ids_to_monitor = ids_to_monitor
        self._last_seen = {id_to_monitor: START_TIME for id_to_monitor in ids_to_monitor}

        self._rabbit = Rabbit(RABBIT_HOST)
        self._heartbeater = HeartBeater(self._rabbit)

        self._rabbit.declare_queue(CONTAINER_ID)
        self._rabbit.route(CONTAINER_ID, HEARTBEAT_EXCHANGE, CONTAINER_ID, self.__on_message_callback)

    def __on_message_callback(self, msg: bytes) -> bool:
        packet = HealthCheck.decode(msg)
        difference = (time.time_ns() - packet.timestamp) / S_TO_NS
        logging.debug(f"Received health check from {packet.id} with {difference} seconds of difference from now")
        if packet.id in self._ids_to_monitor:
            self._last_seen[packet.id] = packet.timestamp

        return True

    @abstractmethod
    def on_check_fail(self, failed_id: str):
        pass

    def __check_health(self):
        for (id_to_monitor, last_seen) in self._last_seen.items():
            if last_seen + S_TO_NS * HEALTH_CHECK_INTERVAL_SEC * 5 < time.time_ns():
                difference = (time.time_ns() - last_seen) / S_TO_NS
                logging.info(f"Container {id_to_monitor} is not responding to health checks for {difference} seconds")
                self.on_check_fail(id_to_monitor)
                self._last_seen[id_to_monitor] = time.time_ns()

        self._rabbit.call_later(HEALTH_CHECK_INTERVAL_SEC, self.__check_health)

    def start(self):
        self._rabbit.call_later(HEALTH_CHECK_INTERVAL_SEC, self.__check_health)
        self._heartbeater.start()
        self._rabbit.start()
