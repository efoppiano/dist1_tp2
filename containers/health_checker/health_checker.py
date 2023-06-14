import os
import logging
import subprocess
import time

from common.utils import initialize_log
from basic_health_checker import BasicHealthChecker

CONTAINERS = os.environ["CONTAINERS"]

class HealthChecker(BasicHealthChecker):
    def on_check_fail(self, container_name: str):

        container_name = f"tp2-{container_name}-1"
        start = time.time()
        subprocess.run(
            ["docker", "start", container_name],
            stdout=subprocess.PIPE, check=False
        )
        end = time.time()
        logging.info("Container %s restarted in %s seconds", container_name, end - start)

        return True
    
    def on_message_callback(self, msg: bytes) -> bool:
        return super().on_message_callback(msg)


def main():
    initialize_log(logging.DEBUG)
    containers = CONTAINERS.split(",")
    health_checker = HealthChecker(containers)
    health_checker.start()


if __name__ == "__main__":
    main()
