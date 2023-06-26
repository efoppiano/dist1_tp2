import os

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

    def start(self):
        pid = os.fork()
        if pid == 0:
            os.execlp("python3",
                      "python3", "/opt/app/common/components/heartbeater/heartbeat_process.py",
                      self._container_id, self._healthchecker, str(self._lapse))
