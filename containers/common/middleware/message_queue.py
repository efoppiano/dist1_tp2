from abc import ABC, abstractmethod


class MessageQueue(ABC):
    @abstractmethod
    def __init__(self, host: str):
        pass

    @abstractmethod
    def publish(self, event: str, message: bytes):
        pass

    @abstractmethod
    def subscribe(self, event: str, callback):
        pass

    @abstractmethod
    def route(self, queue: str, exchange: str, routing_key: str, callback):
        pass

    @abstractmethod
    def send_to_route(self, exchange: str, routing_key: str, message: bytes):
        pass

    @abstractmethod
    def consume(self, queue: str, callback):
        pass

    @abstractmethod
    def produce(self, queue: str, message: bytes):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def declare_queue(self, queue: str):
        pass
