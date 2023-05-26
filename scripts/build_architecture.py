from typing import List, Dict


class EnvVars:
    def __init__(self, env_vars: Dict[str, str]):
        self._env_vars = env_vars

    def add_env_var(self, key: str, value: str):
        self._env_vars[key] = value

    def build_compose_list(self) -> list:
        vars = []
        for key, value in self._env_vars.items():
            vars.append(f"{key}={value}")
        return vars


class ReplicatedFilter:
    def __init__(self, name: str, replicas: int, env_vars: dict = {}):
        self._name = name
        self._replicas = replicas
        self._env_vars = EnvVars(env_vars)

    def build_compose_dict(self, replica_id: int) -> dict:
        return {
            self._name: {
                "build": {
                    "context": f"./containers",
                    "dockerfile": f"{self._name}/Dockerfile",
                },
                "depends_on": {
                    "rabbitmq": {
                        "condition": "service_healthy"
                    }
                },
                "entrypoint": f"python3 /opt/app/{self._name}.py",
                "environment": [
                    "PYTHONHASHSEED=0",
                    f"REPLICA_ID={replica_id}",
                    *self._env_vars.build_compose_list()
                ]
            }
        }

    def build_compose_dicts(self) -> List[dict]:
        dicts = []
        for replica_id in range(self._replicas):
            dicts.append(self.build_compose_dict(replica_id))
        return dicts


def build_rabbit_service_dict() -> dict:
    return {
        "rabbitmq": {
            "build": {
                "context": "./containers/rabbitmq",
                "dockerfile": "Dockerfile",
            },
            "ports": ["5672:5672"],
            "healthcheck": {
                "test": "rabbitmq-diagnostics check_port_connectivity",
                "interval": "5s",
                "timeout": "3s",
                "retries": 10,
                "start_period": "50s",
            }
        }
    }


def main():
    filter = ReplicatedFilter("trips_counter", 3)
    print(filter.build_compose_dicts())


if __name__ == "__main__":
    main()
