import logging
import os
from datetime import datetime, date
from typing import Union


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.addLevelName(
      logging.DEBUG, "\033[1;32m%s\033[1;0m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName(
        logging.INFO, "\033[1;34m%s\033[1;0m" % logging.getLevelName(logging.INFO))
    logging.addLevelName(
        logging.WARNING, "\033[1;33m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName(
        logging.ERROR, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
    logging.addLevelName(
        logging.CRITICAL, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.CRITICAL))

    logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%H:%M:%S')


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


def save_state(state: bytes, path: str = "/volumes/state"):
    # Write to temp file
    with open("/volumes/temp_state", "wb") as f:
        f.write(state)
    # Atomically rename temp file to state file
    os.rename("/volumes/temp_state", path)


def load_state(path: str = "/volumes/state") -> Union[bytes, None]:
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()
    else:
        return None


def hash_msg(msg: bytes) -> int:
    return hash(msg)
