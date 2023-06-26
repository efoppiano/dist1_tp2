import logging
import os
import time
from abc import ABC, abstractmethod

from typing import List

from common.utils import trace
from common.components.heartbeater.heartbeater import HeartBeater
from common.packets.health_check import HealthCheck
from common.middleware.rabbit_middleware import Rabbit

HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")
CONTAINER_ID = os.environ["CONTAINER_ID"]
CONTAINERS = os.environ["CONTAINERS"].split(",")
RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
HEALTH_CHECK_INTERVAL_SEC = os.environ.get("HEALTH_CHECK_INTERVAL_SEC", 2)
GRACE_INTERVALS = os.environ.get("GRACE_INTERVALS", 5)

S_TO_NS = 10 ** 9
START_TIME = time.time_ns()


class BasicHealthChecker(ABC):
    def __init__(self, ids_to_monitor: List[str] = CONTAINERS):
        self._ids_to_monitor = ids_to_monitor
        self._last_seen = {id_to_monitor: START_TIME for id_to_monitor in ids_to_monitor}
        self._last_check = time.time()

        self._rabbit = Rabbit(RABBIT_HOST)
        self._heartbeater = HeartBeater()

        self._rabbit.declare_queue(CONTAINER_ID, durable=False)
        self._rabbit.route(CONTAINER_ID, HEARTBEAT_EXCHANGE, CONTAINER_ID, self.on_message_callback)

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
        logging.debug(
            f"Health check took {end_time - start_time} seconds - after {end_time - self._last_check} seconds")
        self._last_check = end_time

        self._rabbit.call_later(HEALTH_CHECK_INTERVAL_SEC, self.__check_health)

    def start(self):
        self._rabbit.call_later(HEALTH_CHECK_INTERVAL_SEC, self.__check_health)
        self._heartbeater.start()
        self._rabbit.start()
