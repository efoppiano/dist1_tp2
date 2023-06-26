import logging
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from common.utils import initialize_log
from common.middleware.rabbit_middleware import Rabbit
from common.packets.health_check import HealthCheck

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")


class Monitor:
    def __init__(self, container_id: str, health_checker_queue: str, heartbeat_lapse: float):
        self._rabbit = Rabbit(RABBIT_HOST)
        self._container_id = container_id
        self._health_checker_queue = health_checker_queue
        self._heartbeat_lapse = heartbeat_lapse
        self._monitored_pid = os.getppid()

    def __del__(self):
        self._rabbit.close()

    def send_heartbeat(self) -> bool:
        if not self.__is_alive():
            logging.warning(
                f"action: monitor_send_heartbeat | status: container_dead | container_id: {self._container_id}")
            return False

        packet = HealthCheck(
            self._container_id,
            time.time_ns()
        ).encode()

        self._rabbit.send_to_route(HEARTBEAT_EXCHANGE, self._health_checker_queue, packet)
        return True

    def __is_alive(self) -> bool:
        try:
            os.kill(self._monitored_pid, 0)
            return True
        except OSError:
            return False

    def start(self):
        while self.send_heartbeat():
            time.sleep(self._heartbeat_lapse)


def main():
    initialize_log()
    if len(sys.argv) <= 3:
        print("Usage: python3 heartbeat_process.py <container_id> <health_checker_queue> <heartbeat_lapse>")
        exit(1)

    container_id = sys.argv[1]
    health_checker_queue = sys.argv[2]
    try:
        heartbeat_lapse = float(sys.argv[3])
    except ValueError:
        print("heartbeat_lapse must be a float")
        exit(1)

    try:
        logging.debug(
            f"action: monitor_start | status: success | container_id: {container_id} | lapse: {heartbeat_lapse}")
        monitor = Monitor(container_id, health_checker_queue, heartbeat_lapse)
        monitor.start()
    except Exception as e:
        logging.error(
            f"action: monitor_start | status: fail | container_id: {container_id} | error: {e}")

    logging.info("action: monitor_end | status: success")


if __name__ == "__main__":
    main()
