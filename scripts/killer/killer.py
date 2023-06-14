from random import randint
import random
from time import sleep

import docker as docker
import json

RESTAR_CONTAINERS = False

class Killer:
    def __init__(self):
        self._client = docker.from_env()
        self._containers_to_kill = {}
        self.__setup_containers_to_kill()
        self.__remove_blacklisted_containers()

    def __setup_containers_to_kill(self):
        self._containers_to_kill = {}
        with open("deployment.json", "r") as f:
            deployment_data = json.load(f)

        for name, params in deployment_data["containers"].items():
            self._containers_to_kill[name] = params["amount"]

        self._containers_to_kill["response_provider"] = -1

        self._containers_to_kill["health_checker"] = deployment_data["health_chekers"]

    def __remove_blacklisted_containers(self):
        with open("scripts/killer/blacklist.json", "r") as f:
            blacklist_data = json.load(f)

        for name in blacklist_data:
            self._containers_to_kill.pop(name, None)

    def kill_container(self, name: str, replica_id: int) -> str:
        if replica_id == -1:
            container = f"tp2-{name}-1"
        else:
            container = f"tp2-{name}_{replica_id}-1"
        try:
            self._client.containers.get(container).kill(signal="SIGKILL")
        except Exception as e:
            print(f"Failed to kill {container}: {e}")
            pass
        return container

    def start_container(self, name: str, replica_id: int) -> str:
        if replica_id == -1:
            container = f"tp2-{name}-1"
        else:
            container = f"tp2-{name}_{replica_id}-1"
        self._client.containers.get(container).start()
        return container

    def run_kill_loop(self):
        containers_to_kill = list(self._containers_to_kill.keys())
        containers_with_id = []
        for container in containers_to_kill:
            if self._containers_to_kill[container] > 0:
                for replica_id in range(self._containers_to_kill[container]):
                    containers_with_id.append((container, replica_id))
            else:
                containers_with_id.append((container, -1))

        amount_of_containers_to_kill = len(containers_with_id)

        while True:
            amount_to_kill = randint(1, amount_of_containers_to_kill)
            containers_to_kill = random.sample(containers_with_id, amount_to_kill)

            for (container_to_kill, replica_to_kill) in containers_to_kill:
                container = self.kill_container(container_to_kill, replica_to_kill)
                print(f"Killed {container}")

            sleep(2)

            if RESTAR_CONTAINERS:
                for (container_to_kill, replica_to_kill) in containers_to_kill:
                    container = self.start_container(container_to_kill, replica_to_kill)
                    print(f"Started {container}")

            time_to_sleep = random.random() * 15
            print(f"Sleeping for {time_to_sleep} seconds")
            sleep(time_to_sleep)


if __name__ == "__main__":
    killer = Killer()
    killer.run_kill_loop()
