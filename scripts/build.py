import json

### ----------------- SETUP ----------------- ###


with open('deployment.json', 'r') as json_file:
    data = json.load(json_file)


def increase_prev_amount(container_name, amount):
    if "prev_amount" not in data["containers"][container_name]:
        data["containers"][container_name]["prev_amount"] = 0

    data["containers"][container_name]["prev_amount"] += amount


for name, container_data in data["containers"].items():
    next = container_data["next"]

    if isinstance(next, str):
        if next not in data["containers"]:
            next_amount = 1
            continue
        next_amount = data["containers"][next]["amount"]
        if name == "gateway":
            increase_prev_amount(next, 1)
        else:
            increase_prev_amount(next, container_data["amount"])
    else:
        next_amount = {}
        for container_name in next:
            next_amount[container_name] = data["containers"][container_name]["amount"]
            increase_prev_amount(container_name, container_data["amount"])

    container_data["next_amount"] = next_amount

# Health Check
_health_check_containers = []
for name, container_name in data["containers"].items():
    for i in range(container_name["amount"]):
        _health_check_containers.append(f"{name}_{i}")
_health_check_containers.append("response_provider")

HEALTH_CHECKER_AMOUNT = data["health_chekers"]
health_check_containers = []
containers_health_checkers = {}
for i in range(HEALTH_CHECKER_AMOUNT):
    containers = _health_check_containers[i::HEALTH_CHECKER_AMOUNT][:]
    next_n = (i + 1) % HEALTH_CHECKER_AMOUNT

    for container_name in containers:
        containers_health_checkers[container_name] = f"health_checker_{i}"

    containers.append(f"health_checker_{next_n}")
    health_check_containers.append(containers)
### ----------------- RABBIT ----------------- ###


output = '''version: "3.9"
name: tp2
services:

  rabbitmq:
    build:
      context: ./containers/rabbitmq
      dockerfile: Dockerfile
    ports:
      - 15672:15672
    healthcheck:
      test: rabbitmq-diagnostics check_port_connectivity
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 50s'''


### ----------------- CONTAINERS ----------------- ###


def add_container(name, container, n):
    global output

    output += f'''

  {name}_{n}:
    build:
      context: ./containers
      dockerfile: {name}/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    entrypoint: python3 /opt/app/{name}.py
    volumes:
      - .volumes/{name}_{n}:/volumes
    environment:'''

    env = data["common_env"].copy()
    env["INPUT_QUEUE"] = f"{name}_{n}"
    env["EOF_ROUTING_KEY"] = name
    env["HEALTH_CHECKER"] = containers_health_checkers[f"{name}_{n}"]
    env["CONTAINER_ID"] = f"{name}_{n}"

    if "prev_amount" in container:
        env["PREV_AMOUNT"] = container["prev_amount"]
    else:
        env["PREV_AMOUNT"] = 1
    if isinstance(container["next"], str):
        env["NEXT"] = container["next"]

    if "next_amount" in container:
        if isinstance(container["next_amount"], dict):
            for container_name, amount in container["next_amount"].items():
                env[f"NEXT_AMOUNT_{container_name.upper()}"] = amount
        else:
            env["NEXT_AMOUNT"] = container["next_amount"]

    if "env" in container:
        env.update(container["env"])

    for key, value in env.items():
        output += f'''
      - {key}={value}'''


for name, container in data["containers"].items():

    if "amount" not in container:
        amount = 1
    else:
        amount = container["amount"]

    for i in range(amount):
        add_container(name, container, i)


### ----------------- RESPONSE PROVIDER ----------------- ###

def add_response_provider():
    global output
    name = "response_provider"

    output += f'''

  {name}:
    build:
      context: ./containers
      dockerfile: {name}/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    entrypoint: python3 /opt/app/{name}.py
    volumes:
      - .volumes/{name}:/volumes
    environment:'''

    env = data["common_env"].copy()
    env["HEALTH_CHECKER"] = containers_health_checkers[name]
    env["CONTAINER_ID"] = "response_provider"

    for src_type, provider in data["response_provider"].items():
        provider_amount = data["containers"][provider]["amount"]
        env[f"{src_type.upper()}_SRC"] = data["containers"][provider]["next"]
        env[f"{src_type.upper()}_AMOUNT"] = provider_amount

    for key, value in env.items():
        output += f'''
      - {key}={value}'''


add_response_provider()


### ----------------- HEALTH CHECKERS ----------------- ###


def add_health_checker(n, containers):
    global output
    name = "health_checker"
    prev_n = (n - 1) % HEALTH_CHECKER_AMOUNT

    output += f'''

  {name}_{n}:
    build:
      context: ./containers
      dockerfile: {name}/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    entrypoint: python3 /opt/app/{name}.py
    volumes:
      - .volumes/{name}_{n}:/volumes
      - /var/run/docker.sock:/var/run/docker.sock
    environment:'''

    env = data["common_env"].copy()
    env["CONTAINERS"] = ",".join(containers)
    env["HEALTH_CHECKER"] = f"health_checker_{prev_n}"
    env["CONTAINER_ID"] = f"health_checker_{n}"

    for key, value in env.items():
        output += f'''
      - {key}={value}'''


for i in range(HEALTH_CHECKER_AMOUNT):
    containers = health_check_containers[i]
    add_health_checker(i, containers)


### ----------------- CLIENTS ----------------- ###


def add_client(name, cities, data_path):
    global output

    output += f'''

  client_{name}:
    build:
      context: ./containers
      dockerfile: client/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    entrypoint: python3 /opt/app/client.py
    volumes:
      - {data_path}:/opt/app/.data/
    environment:'''

    env = data["common_env"].copy()
    env["DATA_FOLDER_PATH"] = "/opt/app/.data"
    env["CLIENT_ID"] = name
    env["CITIES"] = ",".join(cities)
    env["ID_REQ_QUEUE"] = "client_id_queue"
    env["GATEWAY"] = "gateway"
    env["GATEWAY_AMOUNT"] = data["containers"]["gateway"]["amount"]

    for key, value in env.items():
        output += f'''
      - {key}={value}'''


for name, cities in data["clients"]["clients"].items():
    add_client(name, cities, data["clients"]["data"])

### ----------------- OUTPUT ----------------- ###

with open('docker-compose-dev.yaml', 'w') as file:
    file.write(output)
