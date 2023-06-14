import logging
import signal
from typing import Callable, Union

import pika

from common.message_queue import MessageQueue


class Rabbit(MessageQueue):

    def __init__(self, host: str):
        self._connection_params = pika.ConnectionParameters(host=host, heartbeat=0)
        self.connection = pika.BlockingConnection(self._connection_params)
        self._channel = self.connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._declared_exchanges = []
        self._declared_queues = []
        self._consume_one_last_queue = None

        self.__set_up_signal_handler()
        self._pika_thread = None

    def close(self):
        self.connection.close()
        if self._sig_hand_prev:
            self._sig_hand_prev(signal.SIGTERM, None)
        logging.info("action: rabbit_close | status: success")

    def __set_up_signal_handler(self):
        def signal_handler(_sig, _frame):
            logging.info("action: rabbit_close | status: in_progress")
            self.connection.add_callback_threadsafe(self.close)

        self._sig_hand_prev = signal.signal(signal.SIGTERM, signal_handler)

    def publish(self, event: str, message: bytes):
        self.__declare_exchange(event, "fanout")
        self._channel.basic_publish(exchange=event, routing_key='', body=message)

    @staticmethod
    def __callback_wrapper(callback: Callable[[bytes], bool]):
        def wrapper(ch, method, properties, body):
            if callback(body):
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag)

        return wrapper

    def subscribe(self, event: str, callback: Callable[[bytes], bool]):
        self.__declare_exchange(event, "fanout")
        result = self._channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self._channel.queue_bind(exchange=event, queue=queue_name)
        self._channel.basic_consume(queue=queue_name,
                                    on_message_callback=self.__callback_wrapper(callback),
                                    auto_ack=False)

    def route(self, queue: str, exchange: str, routing_key: str, callback: Union[Callable[[bytes], bool], None] = None):
        self.__declare_exchange(exchange, "direct")
        self.declare_queue(queue)
        self._channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)
        if callback is not None:
            self._channel.basic_consume(queue=queue, on_message_callback=self.__callback_wrapper(callback),
                                        auto_ack=False)

    def send_to_route(self, exchange: str, routing_key: str, message: bytes):
        self.__declare_exchange(exchange, "direct")
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2
            ))

    def consume(self, queue: str, callback: Callable[[bytes], bool]):
        self.declare_queue(queue)
        self._channel.basic_consume(queue=queue, on_message_callback=self.__callback_wrapper(callback), auto_ack=False)

    def consume_one(self, queue: str, callback: Callable[[bytes], bool], cleanup: bool = True):
        if queue != self._consume_one_last_queue:
            if self._consume_one_last_queue is not None:
                self._channel.cancel()
            self._consume_one_last_queue = queue

        self.declare_queue(queue)
        for (method, _, msg) in self._channel.consume(queue=queue, auto_ack=False):
            if callback(msg):
                self._channel.basic_ack(delivery_tag=method.delivery_tag)
                break
            else:
                self._channel.basic_nack(delivery_tag=method.delivery_tag)

        if cleanup:
            self._channel.cancel()

    def consume_until_empty(self, queue: str, callback: Callable[[bytes], bool]):
        '''
        Consumes messages from a queue until it is empty.
        '''
        # TODO: Check if this works
        self.declare_queue(queue)
        while True:
            q = self._channel.queue_declare(queue=queue, passive=True)
            if q.method.message_count == 0:
                break
            self.consume_one(queue, callback, False)
        
        self._channel.cancel()

    def produce(self, queue: str, message: bytes):
        self.declare_queue(queue)
        self._channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2
            ))

    def call_later(self, seconds: float, callback: Callable[[], None]):
        self.connection.call_later(seconds, callback)

    def declare_queue(self, queue: str):
        if queue not in self._declared_queues:
            self._channel.queue_declare(queue=queue, durable=True)
            self._declared_queues.append(queue)

    def __declare_exchange(self, exchange: str, exchange_type: str):
        if exchange not in self._declared_exchanges:
            self._channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
            self._declared_exchanges.append(exchange)

    def start(self):
        self._channel.start_consuming()

    def stop(self):
        self._channel.stop_consuming()
