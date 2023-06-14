import os
import time
from abc import ABC, abstractmethod

from typing import List

from common.heartbeater import HeartBeater
from common.packets.health_check import HealthCheck
from common.rabbit_middleware import Rabbit

HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")
CONTAINER_ID = os.environ["CONTAINER_ID"]
RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
HEALTH_CHECK_QUEUE = os.environ.get("HEALTH_CHECK_QUEUE", "health_check")
HEALTH_CHECK_INTERVAL_SEC = os.environ.get("HEALTH_CHECK_INTERVAL_SEC", 1)


class BasicHealthChecker(ABC):
    def __init__(self, ids_to_monitor: List[str]):
        self._ids_to_monitor = ids_to_monitor
        self._last_seen = {id_to_monitor: None for id_to_monitor in ids_to_monitor}
        self._rabbit = Rabbit(RABBIT_HOST)
        self._heartbeater = HeartBeater(self._rabbit)
        self._rabbit.consume(HEALTH_CHECK_QUEUE, self.__on_message_callback)

    def __on_message_callback(self, msg: bytes) -> bool:
        packet = HealthCheck.decode(msg)
        if packet.id in self._ids_to_monitor:
            self._last_seen[packet.id] = packet.timestamp

        return True

    @abstractmethod
    def on_check_fail(self, failed_id: str):
        pass

    def __check_health(self):
        for (id_to_monitor, last_seen) in self._last_seen.items():
            if last_seen is None:
                self.on_check_fail(id_to_monitor)
            elif last_seen + HEALTH_CHECK_INTERVAL_SEC * 2 < time.time():
                self.on_check_fail(id_to_monitor)

        self._rabbit.call_later(HEALTH_CHECK_INTERVAL_SEC, self.__check_health)

    def run(self):
        self._rabbit.call_later(HEALTH_CHECK_INTERVAL_SEC, self.__check_health)
        self._heartbeater.start()
        self._rabbit.start()
