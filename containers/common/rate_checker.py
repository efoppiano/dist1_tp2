import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests

RABBIT_HOST = os.environ.get("RABBIT_HOST", "rabbitmq")
RABBIT_HTTP_PORT = os.environ.get("RABBIT_HTTP_PORT", 15672)


@dataclass
class Rates:
    publish_per_second: int
    ack_per_second: int


class RateChecker:
    def __init__(self, rabbit_host: str = RABBIT_HOST, rabbit_http_port: int = RABBIT_HTTP_PORT, user: str = "guest",
                 password: str = "guest"):
        self._host = rabbit_host
        self._port = rabbit_http_port
        self._user = user
        self._password = password

    def __get_api_url(self, queue: str) -> str:
        return f"http://{self._host}:{self._port}/api/queues/%2F/{queue}"

    def __get_queue_info(self, queue: str) -> dict:
        auth_header = (self._user, self._password)
        response = requests.get(self.__get_api_url(queue), auth=auth_header)
        return response.json()

    def get_rates(self, queue: str) -> Optional[Rates]:
        api_response = self.__get_queue_info(queue)
        try:
            publish_rate = api_response["message_stats"]["publish_details"]["rate"]
            ack_rate = api_response["message_stats"]["ack_details"]["rate"]
            return Rates(publish_rate, ack_rate)
        except KeyError:
            return None
