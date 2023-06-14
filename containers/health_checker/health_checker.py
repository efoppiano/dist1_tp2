import os
import time
import logging
import subprocess
from common.utils import initialize_log

CONTAINERS = os.environ["CONTAINERS"]


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


def start_container(container_name):
    """Starts a container with the given name"""
    container_name = f"tp2-{container_name}-1"
    result = subprocess.run(
        ["docker", "start", container_name],
        stdout=subprocess.PIPE, check=False
    )
    if result.returncode != 0:
        logging.error("Error starting container %s", container_name)
        return False

    return True


class HealthChecker:
    """Checks if the containers are alive and starts them if they are not"""

    def __init__(self, containers):
        self.containers = containers

    def start(self):
        """Starts the health checker"""
        for container in self.containers:
            if not is_alive(container):
                start_container(container)

        time.sleep(5)


def main():
    """Main function"""
    initialize_log()
    # split CONTAINERS by comma
    containers = CONTAINERS.split(",")
    health_checker = HealthChecker(containers)
    health_checker.start()


if __name__ == "__main__":
    main()
