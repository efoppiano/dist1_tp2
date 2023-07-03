import csv
import json
import logging
import os
import random
from typing import Protocol, TextIO

ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
DIRECTORY = os.environ.get("DIRECTORY", "/volumes/state")
LOG_FILE_NAME = os.environ.get("LOG_FILE_NAME", "log")
STATE_FILE_NAME = os.environ.get("STATE_FILE_NAME", "state")
CHANCE_OF_CHECKPOINT = float(os.environ.get("CHANCE_OF_CHECKPOINT", "0.2"))
CHECKPOINT = "checkpoint"
COLUMN_PARTITION_SIZE = 4


def fail_random():
    if random.random() < 0.0000001:
        exit(137)


class BuggyWriter:
    def __init__(self, file: TextIO):
        self._writer = csv.writer(file)

    def writerow(self, row: list):
        if random.random() < 0.00001:
            logging.debug("BuggyWriter: Dropping part of the row")
            items_to_write = random.randint(0, len(row))
            if items_to_write == 0:
                exit(137)
            try:
                bytes_of_last_item_to_write = random.randint(0, len(row[items_to_write - 1]))

                row[items_to_write - 1] = row[items_to_write - 1][:bytes_of_last_item_to_write]

                row = row[:items_to_write]

                self._writer.writerow(row)
            except Exception as e:
                logging.debug(f"Cannot truncate last item - {row[items_to_write - 1]} - {e}")
            finally:
                exit(137)
        else:
            self._writer.writerow(row)

    def __getattr__(self, name):
        return getattr(self._writer, name)


class Recoverable(Protocol):
    def get_state(self) -> dict:
        pass

    def set_state(self, state: dict) -> None:
        pass

    def replay(self, msg: bytes) -> None:
        pass


class StateSaver:
    def __init__(self, component: Recoverable, chance_of_checkpoint: float = CHANCE_OF_CHECKPOINT):
        self._component = component
        self._chance_of_checkpoint = chance_of_checkpoint
        self._log_file = None
        self._log_writer = None

        self.__init_paths()
        self.__load_state()

    def __open_log_file(self):
        if self._log_file and not self._log_file.closed:
            return

        if self.__log_exists():
            self._log_file = open(self._log_file_path, "a")
        else:
            self._log_file = open(self._log_file_path, "w")

        self._log_writer = BuggyWriter(self._log_file)

    def __init_paths(self):
        self._log_file_path = os.path.join(DIRECTORY, LOG_FILE_NAME)
        self._tmp_log_file_path = os.path.join(DIRECTORY, f"{LOG_FILE_NAME}.tmp")
        self._truncated_log_file_path = os.path.join(DIRECTORY, f"{LOG_FILE_NAME}.truncated")
        self._state_file_path = os.path.join(DIRECTORY, STATE_FILE_NAME)
        self._tmp_state_file_path = os.path.join(DIRECTORY, f"{STATE_FILE_NAME}.tmp")
        self._directory = DIRECTORY
        os.makedirs(self._directory, exist_ok=True)

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
            if self._log_file and not self._log_file.closed:
                fail_random()
                self._log_file.close()
            fail_random()
            os.rename(self._log_file_path, self._tmp_log_file_path)
            os.sync()
            fail_random()
            os.remove(self._tmp_log_file_path)
            os.sync()
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
        with open(self._state_file_path, "r") as f:
            self._component.set_state(json.loads(f.read()))
            fail_random()

    def __load_from_checkpoint(self):
        if self.__tmp_state_exists():
            os.rename(self._tmp_state_file_path, self._state_file_path)
        fail_random()
        self.__load_from_state()
        fail_random()
        self.__remove_log()
        fail_random()
        self.__open_log_file()
        fail_random()

    def __replay_valid_lines(self):
        error_lines = []
        i = 0
        with open(self._log_file_path, "r") as log_file, open(self._truncated_log_file_path,
                                                              "w") as truncate_log_file:
            log_reader = csv.reader(log_file)
            fail_random()
            log_truncated_writer = BuggyWriter(truncate_log_file)
            fail_random()
            for row in log_reader:
                if len(row) < 2:
                    error_lines.append(i)
                    continue

                try:
                    msg_size, *msg_rows = row
                    msg = "".join(msg_rows)
                    msg = bytes.fromhex(msg)
                except ValueError as e:
                    logging.warning(f"Failed to parse line {i} - {e}")
                    error_lines.append(i)
                else:
                    if int(msg_size) == len(msg):
                        self._component.replay(msg)
                        log_truncated_writer.writerow(row)
                    else:
                        error_lines.append(i)

                i += 1
            fail_random()
            truncate_log_file.flush()
            os.fsync(truncate_log_file.fileno())
            if ENVIRONMENT != "dev":
                os.fsync(truncate_log_file.fileno())

        fail_random()
        if len(error_lines) > 0:
            logging.warning(f"Found {len(error_lines)}/{i} invalid lines in the log file - {error_lines}")

        logging.info("Replayed valid lines, truncating log file")
        os.rename(self._truncated_log_file_path, self._log_file_path)
        os.sync()

    def __save_checkpoint(self):
        # get the updated state from the component
        state = self._component.get_state()
        fail_random()

        # write the state to a tmp file
        with open(self._tmp_state_file_path, "w") as f:
            fail_random()
            f.write(json.dumps(state))
            fail_random()
            f.flush()
            os.fsync(f.fileno())
            if ENVIRONMENT != "dev":
                os.fsync(f.fileno())

        try:
            # write the checkpoint to the log
            fail_random()
            self._log_writer.writerow([CHECKPOINT])
            fail_random()
            self._log_file.flush()
            os.fsync(self._log_file.fileno())
            if ENVIRONMENT != "dev":
                os.fsync(self._log_file.fileno())
        except (AttributeError, ValueError):  # log file was closed
            logging.warning("Log file was closed, reopening")
            self.__open_log_file()
            fail_random()
            self._log_writer.writerow([CHECKPOINT])
            fail_random()
            self._log_file.flush()
            os.fsync(self._log_file.fileno())
            if ENVIRONMENT != "dev":
                os.fsync(self._log_file.fileno())
        fail_random()
        # rename the tmp state file to the state file
        os.rename(self._tmp_state_file_path, self._state_file_path)
        os.sync()

        # remove the log file
        self.__remove_log()

    def __append_to_log(self, msg: bytes):
        if self._log_file is None:
            self.__open_log_file()

        new_msg_size = len(msg)

        row = [msg[i:i + COLUMN_PARTITION_SIZE].hex() for i in range(0, len(msg), COLUMN_PARTITION_SIZE)]
        self._log_writer.writerow([new_msg_size, *row])
        fail_random()
        self._log_file.flush()
        os.fsync(self._log_file.fileno())
        if ENVIRONMENT != "dev":
            os.fsync(self._log_file.fileno())

    def save_state(self, new_msg: bytes):
        fail_random()
        self.__append_to_log(new_msg)
        fail_random()
        # This could be done in an exact manner, keeping a counter in memory
        # remembering to do a checkpoint on startup (so we don't lose it on crash)
        do_checkpoint = self._chance_of_checkpoint > random.random()
        fail_random()
        if do_checkpoint:
            fail_random()
            self.__save_checkpoint()
