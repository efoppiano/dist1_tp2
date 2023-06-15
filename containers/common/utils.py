import logging
import os
import json
from datetime import datetime, date
from typing import Union


def initialize_log(logging_level=logging.INFO):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.getLogger("pika").setLevel(logging.WARNING)

    logging.TRACE = 5
    logging.addLevelName(
        logging.TRACE, "\033[0;36mTRACE\033[1;0m")

    logging.addLevelName(
        logging.DEBUG, "\033[0;35m%s\033[1;0m" % logging.getLevelName(logging.DEBUG))
    
    logging.MISSING = 14
    logging.addLevelName(
        logging.MISSING, "\033[0;33mMISSING\033[1;0m")
    
    logging.MSG = 16
    logging.addLevelName(
        logging.MSG, "\033[0;32mMSG\033[1;0m")

    logging.addLevelName(
        logging.INFO, "\033[0;34m%s\033[1;0m" % logging.getLevelName(logging.INFO))

    logging.DUPLICATE = 22
    logging.addLevelName(
        logging.DUPLICATE, "\033[1;95mDUPL\033[1;0m")

    logging.EVICT = 24
    logging.addLevelName(
        logging.EVICT, "\033[1;96mEVICT\033[1;0m")

    logging.SUCCESS = 28
    logging.addLevelName(
        logging.SUCCESS, "\033[1;92mSUCCESS\033[1;0m")

    logging.addLevelName(
        logging.WARNING, "\033[1;93m%s\033[1;0m" % logging.getLevelName(logging.WARNING))

    logging.addLevelName(
        logging.ERROR, "\033[1;91m%s\033[1;0m" % logging.getLevelName(logging.ERROR))

    logging.addLevelName(
        logging.CRITICAL, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.CRITICAL))

    logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%H:%M:%S')


def trace(message, *args, **kws):
    logging.log(logging.TRACE, message, *args, **kws)

def log_missing(message, *args, **kws):
    logging.log(logging.MISSING, message, *args, **kws)

def log_msg(message, *args, **kws):
    logging.log(logging.MSG, message, *args, **kws)

def log_duplicate(message, *args, **kws):
    logging.log(logging.DUPLICATE, message, *args, **kws)

def log_evict(message, *args, **kws):
    logging.log(logging.EVICT, message, *args, **kws)


def success(message, *args, **kws):
    logging.log(logging.SUCCESS, message, *args, **kws)

def min_hash(obj, n=4, min_log_level = logging.DEBUG):

    # This function is specific for debbuging purposes
    if logging.getLogger().getEffectiveLevel() > min_log_level:
        return "###"

    try:
        return str(hash(obj))[-n:]
    except:
        return "###"
    
 
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
    with open("/volumes/temp_state", "wb", buffering=0) as f:
        f.write(state)
        f.flush()
        os.fsync(f.fileno())

    # Atomically rename temp file to state file
    os.rename("/volumes/temp_state", path)
    os.sync()


def load_state(path: str = "/volumes/state") -> Union[bytes, None]:
    if os.path.exists(path):
        with open(path, "rb") as f:
            return f.read()
    else:
        return None


def json_serialize(obj):
    try:
        return json.dumps(obj, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    except TypeError:
        return json.dumps(obj, default=lambda o: str(o), sort_keys=True, indent=4)
