import logging
import os
import signal

from common.utils import append_signal

HEARTBEAT_EXCHANGE = os.environ.get("HEARTBEAT_EXCHANGE", "healthcheck")
CONTAINER_ID = os.environ["CONTAINER_ID"]
HEALTHCHECKER = os.environ["HEALTH_CHECKER"]
LAPSE = int(os.environ.get("HEARTBEAT_LAPSE", 1))
RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")


class HeartBeater:
    def __init__(self, container_id: str = CONTAINER_ID, healthchecker: str = HEALTHCHECKER,
                 lapse: int = LAPSE) -> None:
        self._container_id = container_id
        self._healthchecker = healthchecker
        self._lapse = lapse
        self._child_pid = None

        self.__setup_signal_handler()

    def __setup_signal_handler(self):
        def handler(_sig, _frame):
            logging.info("action: heartbeater_stop | status: in_progress")
            if self._child_pid:
                os.kill(self._child_pid, signal.SIGTERM)
                os.waitpid(self._child_pid, 0)
            logging.info("action: heartbeater_stop | status: success")

        append_signal(signal.SIGTERM, handler)

    def start(self):
        pid = os.fork()
        if pid > 0:
            self._child_pid = pid
        else:
            os.execlp("python3",
                      "python3", "/opt/app/common/components/heartbeater/heartbeat_process.py",
                      self._container_id, self._healthchecker, str(self._lapse))
