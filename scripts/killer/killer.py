from random import randint
import random
from time import sleep

import docker as docker
import yaml


class Killer:
    def __init__(self):
        self._client = docker.from_env()
        self._containers_to_kill = self.__setup_containers_to_kill()

    def __setup_containers_to_kill(self):
        with open("scripts/killer/containers.yaml", "r") as f:
            return yaml.safe_load(f)

    def kill_container(self, name: str, replica_id: int):
        container = f"tp2-{name}_{replica_id}-1"
        try:
            self._client.containers.get(container).kill(signal="SIGKILL")
        except Exception as e:
            print(f"Failed to kill {container}: {e}")
            pass

    def start_container(self, name: str, replica_id: int):
        container = f"tp2-{name}_{replica_id}-1"
        self._client.containers.get(container).start()

    def run_kill_loop(self):
        containers_to_kill = list(self._containers_to_kill.keys())
        amount_of_containers_to_kill = sum(self._containers_to_kill.values())
        containers_with_id = [(container, replica_id) for container in containers_to_kill for replica_id in
                              range(self._containers_to_kill[container])]

        while True:
            amount_to_kill = randint(1, amount_of_containers_to_kill)
            containers_to_kill = random.sample(containers_with_id, amount_to_kill)

            for (container_to_kill, replica_to_kill) in containers_to_kill:
                self.kill_container(container_to_kill, replica_to_kill)
                print(f"Killed {container_to_kill}_{replica_to_kill}")

            sleep(2)

            for (container_to_kill, replica_to_kill) in containers_to_kill:
                self.start_container(container_to_kill, replica_to_kill)
                print(f"Started {container_to_kill}_{replica_to_kill}")

            time_to_sleep = random.random() * 15
            print(f"Sleeping for {time_to_sleep} seconds")
            sleep(time_to_sleep)


if __name__ == "__main__":
    killer = Killer()
    killer.run_kill_loop()
