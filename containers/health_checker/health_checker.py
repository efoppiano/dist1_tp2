import os
import time
import logging
import subprocess
from common.utils import initialize_log


CONTAINERS = os.environ["CONTAINERS"]

def is_alive(container_name):
    '''Checks if a container with the given name is alive'''
    result = subprocess.run(
        ["docker", "inspect", container_name],
        stdout=subprocess.PIPE, check=False
    )
    if result.returncode != 0:
        return False

    return True

def start_container(container_name):
    '''Starts a container with the given name'''
    result = subprocess.run(
        ["docker", "start", container_name],
        stdout=subprocess.PIPE, check=False
    )
    if result.returncode != 0:
        logging.error("Error starting container %s", container_name)
        return False

    return True


class HealthChecker:
    '''Checks if the containers are alive and starts them if they are not'''
    def __init__(self, containers):
        self.containers = containers

    def start(self):
        '''Starts the health checker'''
        for container in self.containers:
            if not is_alive(container):
                logging.info("Container %s is not alive, starting it", container)
                start_container(container)
            else:
                logging.info("Container %s is alive", container)

        time.sleep(5)


def main():
    '''Main function'''
    initialize_log()
    # slip CONTAINERS by comma
    containers = CONTAINERS.split(",")
    health_checker = HealthChecker(containers)
    health_checker.start()


if __name__ == "__main__":
    main()
