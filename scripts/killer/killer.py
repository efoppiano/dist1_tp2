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
            self._client.containers.get(container).start()
        except Exception as e:
            print(f"Failed to kill {container}: {e}")
            pass

    def run_kill_loop(self):
        while True:
            container_to_kill = random.choice(list(self._containers_to_kill.keys()))
            replicas = self._containers_to_kill[container_to_kill]
            replica_to_kill = randint(0, replicas - 1)
            self.kill_container(container_to_kill, replica_to_kill)
            print(f"Killed {container_to_kill}_{replica_to_kill}")

            if random.random() < 0.2:
                time_to_sleep = random.random() * 20
                print(f"Sleeping for {time_to_sleep} seconds")
                sleep(time_to_sleep)


if __name__ == "__main__":
    killer = Killer()
    killer.run_kill_loop()
