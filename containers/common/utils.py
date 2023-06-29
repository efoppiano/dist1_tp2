import logging
import os
import json
import signal
from datetime import datetime, date
from types import FrameType
from typing import Union, Callable

RESULTS_ROUTING_KEY = "results"
PUBLISH_ROUTING_KEY = "publish"


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


def min_hash(obj, n=4, min_log_level=logging.DEBUG):
    # This function is specific for debbuging purposes
    if logging.getLogger().getEffectiveLevel() > min_log_level:
        return "###"

    try:
        return str(hash(obj))[-n:]
    except:
        return "###"


def bold(text: str) -> str:
    return f"\033[1m{text}\033[0m"


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


def append_signal(sig, f: Callable[[signal.Signals, FrameType], None]):
    old = None
    if callable(signal.getsignal(sig)):
        old = signal.getsignal(sig)

    def helper(signum: signal.Signals, frame: FrameType):
        if old is not None:
            old(signum, frame)
        f(signum, frame)

    signal.signal(sig, helper)


def build_results_queue_name(destination: str) -> str:
    return f"results_{destination}"


def build_healthcheck_container_id(container_id: str) -> str:
    return f"{container_id}_healthcheck"


def build_control_queue_name(client_id: str) -> str:
    return f"control_{client_id}"
