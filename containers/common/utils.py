import logging
from datetime import datetime, date
from typing import Union


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def build_queue_name(queue: str, id: Union[int, None] = None) -> str:
    if id is None:
        return queue
    else:
        return f"{queue}_{id}"


def build_prefixed_queue_name(prefix: str, suffix: str, id: Union[int, None] = None) -> str:
    if id is None:
        return f"{prefix}_{suffix}"
    else:
        return f"{prefix}_{suffix}_{id}"


def build_prefixed_hashed_queue_name(prefix: str, suffix: str, key: str, output_amount: int) -> str:
    return f"{prefix}_{suffix}_{hash(key) % output_amount}"


def build_hashed_queue_name(queue: str, key: str, output_amount: int) -> str:
    return f"{queue}_{hash(key) % output_amount}"


def build_eof_in_queue_name(prefix: str, suffix: Union[str, None] = None) -> str:
    if suffix is None:
        return f"{prefix}_eof_in"
    return f"{prefix}_{suffix}_eof_in"


def build_eof_out_queue_name(prefix: str, suffix: Union[str, None] = None) -> str:
    if suffix is None:
        return f"{prefix}_eof_out"
    return f"{prefix}_{suffix}_eof_out"


def parse_date(date_str: str) -> date:
    return datetime.strptime(date_str, "%Y-%m-%d")


def datetime_str_to_date_str(datetime_str: str) -> str:
    return datetime_str.split(" ")[0]
