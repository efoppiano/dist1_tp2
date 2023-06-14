from typing import Dict, Tuple, List, Union


class Router:
    def __init__(self, queue_name: str, amount: Union[int, None]):
        self.queue_name = queue_name
        self.amount = amount

    def route(self, hashing_key: str = None) -> str:
        if self.amount is None:
            return f"{self.queue_name}"

        queue_num = hash(hashing_key) % self.amount
        return f"{self.queue_name}_{queue_num}"

    def publish(self) -> str:
        return f"publish_{self.queue_name}"


class MultiRouter:
    def __init__(self, queues: Dict[str, Tuple[str, int]]):
        self.queues = queues

    def route(self, queue: str, hashing_key: str) -> str:
        (queue, amount) = self.queues.get(queue)

        queue_num = hash(hashing_key) % amount
        return f"{queue}_{queue_num}"

    def publish(self) -> List[str]:
        queues = []
        for (_, (queue, _)) in self.queues.items():
            queues.append(f"publish_{queue}")

        return queues
