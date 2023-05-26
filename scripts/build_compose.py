import copy
import json

import yaml


def main():
    with open("scripts/docker-compose-layout.yaml", "r") as f:
        compose_dict = yaml.safe_load(f)

    with open("scripts/layout.yaml", "r") as f:
        layout_dict = yaml.safe_load(f)

    services_with_replicas = {
        "version": compose_dict.pop("version"),
        "name": compose_dict.pop("name"),
        "services": {}
    }

    for (service_name, service_data) in compose_dict["services"].items():
        if service_name in layout_dict:
            for replica_id in range(layout_dict[service_name]["amount"]):
                new_service_name = f"{service_name}_{replica_id}"
                services_with_replicas["services"][new_service_name] = copy.deepcopy(service_data)
                services_with_replicas["services"][new_service_name]["restart"] = "on-failure"
                services_with_replicas["services"][new_service_name].setdefault("environment", [])
                services_with_replicas["services"][new_service_name]["environment"].append(f"REPLICA_ID={replica_id}")

                if layout_dict[service_name]["add_volume"]:
                    services_with_replicas["services"][new_service_name].setdefault("volumes", [])
                    services_with_replicas["services"][new_service_name]["volumes"].append(
                        f".volumes/{new_service_name}:/volumes")

        else:
            services_with_replicas["services"][service_name] = service_data.copy()
            services_with_replicas["services"][service_name]["restart"] = "on-failure"

    with open("docker-compose-dev.yaml", "w") as f:
        yaml.dump(services_with_replicas, f, sort_keys=False)


if __name__ == "__main__":
    main()
