import json
import logging
from typing import Union, Tuple

DEFAULT_CONFIG_PATH = "/opt/app/common/linker/config.json"


# Implementation of the singleton pattern
# Source: https://refactoring.guru/design-patterns/singleton/python/example#example-0
# The application is single threaded, so we can use the simplest implementation
class LinkerMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Linker(metaclass=LinkerMeta):
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH

        logging.info(f"action: linker_init | status: success | config_path: {config_path}")
        self._queues = self.__load_config(config_path)

    @staticmethod
    def __load_config(config_path: str) -> dict:
        with open(config_path, "r") as f:
            config = json.load(f)

        return config

    def get_input_queue(self, curr_node: object, replica_id: int, prev_node_name: str = None) -> str:
        curr_node_name = curr_node.__class__.__name__
        queue_prefix, link_info = self.__search_for_link(prev_node_name, curr_node_name)
        return f"{queue_prefix}_{replica_id}"

    @staticmethod
    def __get_replica_id(hashing_key: str, out_amount: int) -> int:
        return hash(hashing_key) % out_amount

    def __search_for_link_by_both_nodes(self, left_node: str, right_node: str) -> Tuple[str, dict]:
        for (queue_prefix, link_info) in self._queues.items():
            if link_info["left"] == left_node and link_info["right"] == right_node:
                return queue_prefix, link_info

        raise ValueError(f"Could not find link for {left_node} -> {right_node}")

    def __search_for_link_by_left_node(self, left_node: str) -> Tuple[str, dict]:
        found = False
        results = []
        for (queue_prefix, link_info) in self._queues.items():
            if link_info["left"] == left_node:
                if found:
                    raise ValueError(f"Found multiple values of <NODE> for '{left_node} -> <NODE>' but"
                                     f" expected only one")
                found = True
                results.append((queue_prefix, link_info))

        if not found:
            raise ValueError(f"Tried to find <NODE> in '{left_node} -> <NODE>' but could not find it")

        return results[0]

    def __search_for_link_by_right_node(self, right_node: str) -> Tuple[str, dict]:
        found = False
        results = []
        for (queue_prefix, link_info) in self._queues.items():
            if link_info["right"] == right_node:
                if found:
                    raise ValueError(f"Found multiple values of <NODE> for '<NODE> -> {right_node}' but"
                                     f" expected only one")
                found = True
                results.append((queue_prefix, link_info))

        if not found:
            raise ValueError(f"Tried to find <NODE> in '<NODE> -> {right_node}' but could not find it")

        return results[0]

    def __search_for_link(self, left_node: Union[str, None], right_node: Union[str, None]) -> Tuple[str, dict]:
        if left_node is None and right_node is None:
            raise ValueError("Must provide at least one of left_node and right_node")

        if left_node is None:
            return self.__search_for_link_by_right_node(right_node)
        elif right_node is None:
            return self.__search_for_link_by_left_node(left_node)
        else:
            return self.__search_for_link_by_both_nodes(left_node, right_node)

    def get_output_queue(self, curr_node: object, next_node_name: str = None,
                         hashing_key: str = None) -> str:
        curr_node_name = curr_node.__class__.__name__
        queue_prefix, link_info = self.__search_for_link(curr_node_name, next_node_name)
        if hashing_key is None:
            if link_info["out_amount"] != 1:
                raise ValueError(f"Must provide hashing key for {curr_node_name} -> {next_node_name} because "
                                 f"{next_node_name} has {link_info['out_amount']} replicas")
            return queue_prefix
        replica_id = self.__get_replica_id(hashing_key, link_info["out_amount"])
        return f"{queue_prefix}_{replica_id}"

    def get_eof_in_queue(self, curr_node: object, next_node_name: str = None) -> str:
        curr_node_name = curr_node.__class__.__name__
        queue_prefix, _ = self.__search_for_link(curr_node_name, next_node_name)
        return f"{queue_prefix}_eof_in"

    def get_eof_out_routing_key(self, curr_node: object, prev_node_name: str = None) -> str:
        curr_node_name = curr_node.__class__.__name__
        queue_prefix, _ = self.__search_for_link(prev_node_name, curr_node_name)
        return f"{queue_prefix}_eof_out"
