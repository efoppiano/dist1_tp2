import os
import logging
import subprocess
from common.utils import initialize_log
from common.basic_health_checker import BasicHealthChecker

CONTAINERS = os.environ["CONTAINERS"]

# ! FIXME: REMOVE THIS - ONLY FOR DEBUGGING
def is_alive(container_name):
    """Checks if a container with the given name is alive by its status"""
    container_name = f"tp2-{container_name}-1"
    result = subprocess.run(
        ["docker", "inspect", container_name, "--format='{{.State.Status}}'"],
        stdout=subprocess.PIPE, check=False
    )
    if result.returncode != 0:
        return False

    status = result.stdout.decode("utf-8").strip()
    logging.info("Container %s is %s", container_name, status)

    if status == "'exited'":
        return False

    return True


class HealthChecker(BasicHealthChecker):

    def on_check_fail(self, container_name: str):

        if is_alive(container_name):
            logging.critical("Container %s is already alive", container_name)

        container_name = f"tp2-{container_name}-1"
        result = subprocess.run(
            ["docker", "start", container_name],
            stdout=subprocess.PIPE, check=False
        )
        if result.returncode != 0:
            logging.error("Error starting container %s", container_name)
            return False
        
        logging.info("Container %s started", container_name)
        return True
    
    def on_message_callback(self, msg: bytes) -> bool:
        logging.info("Received message: %s", msg)
        return super().on_message_callback(msg)


def main():
    initialize_log()
    containers = CONTAINERS.split(",")
    health_checker = HealthChecker(containers)
    health_checker.start()


if __name__ == "__main__":
    main()
