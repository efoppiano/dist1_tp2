import csv
import logging
import os
import random
import signal
from typing import Protocol

from common.utils import append_signal

DIRECTORY = os.environ.get("DIRECTORY", "/volumes/state")
LOG_FILE_NAME = os.environ.get("LOG_FILE_NAME", "log")
STATE_FILE_NAME = os.environ.get("STATE_FILE_NAME", "state")
CHANCE_OF_CHECKPOINT = float(os.environ.get("CHANCE_OF_CHECKPOINT", "0.2"))
CHECKPOINT = "checkpoint"
COLUMN_PARTITION_SIZE = 1024


class Recoverable(Protocol):
    def get_state(self) -> bytes:
        pass

    def set_state(self, state: bytes) -> None:
        pass

    def replay(self, msg: bytes) -> None:
        pass


class StateSaver:
    def __init__(self, component: Recoverable, chance_of_checkpoint: float = CHANCE_OF_CHECKPOINT):
        self._component = component
        self._chance_of_checkpoint = chance_of_checkpoint

        self.__init_paths()
        self.__load_state()

        os.makedirs(self._directory, exist_ok=True)
        self._log_file = None
        self._log_writer = None
        self.__setup_signal_handler()

    def __del__(self):
        if self._log_file is not None:
            self._log_file.close()

    def __setup_signal_handler(self):
        def signal_handler(_sig, _frame):
            logging.info("action: state_saver_close | status: in_progress")
            self.__save_checkpoint()
            logging.info("action: state_saver_close | status: success")

        append_signal(signal.SIGTERM, signal_handler)

    def __open_log_file(self):
        if self.__log_exists():
            self._log_file = open(self._log_file_path, "a")
        else:
            self._log_file = open(self._log_file_path, "w")

        self._log_writer = csv.writer(self._log_file)

    def __init_paths(self):
        self._log_file_path = os.path.join(DIRECTORY, LOG_FILE_NAME)
        self._tmp_log_file_path = os.path.join(DIRECTORY, f"{LOG_FILE_NAME}.tmp")
        self._truncated_log_file_path = os.path.join(DIRECTORY, f"{LOG_FILE_NAME}.truncated")
        self._state_file_path = os.path.join(DIRECTORY, STATE_FILE_NAME)
        self._tmp_state_file_path = os.path.join(DIRECTORY, f"{STATE_FILE_NAME}.tmp")
        self._directory = DIRECTORY

    def __load_state(self):
        if self.__log_exists():
            self.__load_state_with_log()
        elif self.__state_exists():
            self.__load_from_state()

    def __log_exists(self) -> bool:
        return os.path.exists(self._log_file_path)

    def __state_exists(self) -> bool:
        return os.path.exists(self._state_file_path)

    def __tmp_state_exists(self) -> bool:
        return os.path.exists(self._tmp_state_file_path)

    def __last_line_is_checkpoint(self) -> bool:
        with open(self._log_file_path, "r") as f:
            log_reader = csv.reader(f)
            last_line = None
            for row in log_reader:
                last_line = row

            if last_line is None or len(last_line) == 0:
                return False

            return last_line[0].startswith(CHECKPOINT)

    def __remove_log(self):
        try:
            self._log_file.close()
            os.rename(self._log_file_path, self._tmp_log_file_path)
            os.remove(self._tmp_log_file_path)
        except FileNotFoundError:
            pass

        self._log_file = None
        self._log_writer = None

    def __load_state_with_log(self):
        if self.__state_exists():
            if self.__last_line_is_checkpoint():
                self.__load_from_checkpoint()
                logging.info("Loaded from checkpoint")
            else:
                self.__load_from_state()
                self.__replay_valid_lines()
        else:
            self.__replay_valid_lines()

    def __load_from_state(self):
        with open(self._state_file_path, "rb") as f:
            self._component.set_state(f.read())

    def __load_from_checkpoint(self):
        self.__remove_log()
        self.__open_log_file()

        if self.__tmp_state_exists():
            os.rename(self._tmp_state_file_path, self._state_file_path)
            self.__load_from_state()

    def __replay_valid_lines(self):
        error_lines = []
        i = 0
        with open(self._log_file_path, "r") as log_file, open(self._truncated_log_file_path,
                                                              "w") as truncate_log_file:
            log_reader = csv.reader(log_file)
            log_truncated_writer = csv.writer(truncate_log_file)

            for row in log_reader:
                if len(row) < 2:
                    error_lines.append(i)
                    continue

                msg_size, *msg_rows = row
                msg = "".join(msg_rows)
                msg = bytes.fromhex(msg)

                if int(msg_size) == len(msg):
                    self._component.replay(msg)
                    log_truncated_writer.writerow(row)
                else:
                    error_lines.append(i)

                i += 1

            truncate_log_file.flush()

        if len(error_lines) > 0:
            logging.warning(f"Found {len(error_lines)}/{i} invalid lines in the log file - {error_lines}")

        logging.info("Replayed valid lines, truncating log file")
        os.rename(self._truncated_log_file_path, self._log_file_path)

    def __save_checkpoint(self):
        # get the updated state from the component
        state = self._component.get_state()

        # write the state to a tmp file
        with open(self._tmp_state_file_path, "wb") as f:
            f.write(state)
            f.flush()

        if self._log_file is None:
            self.__open_log_file()

        # write the checkpoint to the log
        self._log_writer.writerow([CHECKPOINT])
        self._log_file.flush()

        # rename the tmp state file to the state file
        os.rename(self._tmp_state_file_path, self._state_file_path)

        # remove the log file
        self.__remove_log()

    def __append_to_log(self, msg: bytes):
        if self._log_file is None:
            self.__open_log_file()

        new_msg_size = len(msg)

        row = [msg[i:i + COLUMN_PARTITION_SIZE].hex() for i in range(0, len(msg), COLUMN_PARTITION_SIZE)]
        self._log_writer.writerow([new_msg_size, *row])
        self._log_file.flush()

    def save_state(self, new_msg: bytes):
        self.__append_to_log(new_msg)

        # This could be done in an exact manner, keeping a counter in memory
        # remembering to do a checkpoint on startup (so we don't lose it on crash)
        do_checkpoint = self._chance_of_checkpoint > random.random()
        if do_checkpoint:
            self.__save_checkpoint()


def replay_valid_lines():
    error_lines = []
    i = 0
    with open("log", "r") as log_file:
        log_reader = csv.reader(log_file)

        for line in log_reader:
            print(f"Line: {line}")
            msg_size, *msg_rows = line
            print(msg_rows)
            msg = "".join(msg_rows)
            msg = bytes.fromhex(msg)
            print(f"Msg: {msg}")

            if int(msg_size) == len(msg):
                logging.info(f"Replaying {msg}")
            else:
                error_lines.append(i)
            i += 1

    if len(error_lines) > 0:
        logging.warning(f"Found {len(error_lines)}/{i} invalid lines in the log file - {error_lines}")