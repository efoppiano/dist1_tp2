import os
import random
from typing import Protocol

DIRECTORY = os.environ.get("DIRECTORY", "/volumes/state")
LOG_FILE_NAME = os.environ.get("LOG_FILE_NAME", "log")
STATE_FILE_NAME = os.environ.get("STATE_FILE_NAME", "state")
CHANCE_OF_CHECKPOINT = float(os.environ.get("CHANCE_OF_CHECKPOINT", "0.05"))
CHECKPOINT = "checkpoint"


class Recoverable(Protocol):
    def get_state(self) -> bytes:
        pass

    def set_state(self, state: bytes) -> None:
        pass

    def replay(self, msg: bytes) -> None:
        pass


class StateSaver:
    def __init__(self, component: Recoverable, max_log_size: int):
        self._max_log_size = max_log_size
        self._component = component

        self.__init_paths()
        self.__load_state()

        self._log_file = open(self._log_file_path, "a")

    def __del__(self):
        self._log_file.close()

    def __init_paths(self):
        self._log_file_path = os.path.join(DIRECTORY, LOG_FILE_NAME)
        self._tmp_log_file_path = os.path.join(DIRECTORY, f"{LOG_FILE_NAME}.tmp")
        self._truncated_log_file_path = os.path.join(DIRECTORY, f"{LOG_FILE_NAME}.truncated")
        self._state_file_path = os.path.join(DIRECTORY, STATE_FILE_NAME)
        self._tmp_state_file_path = os.path.join(DIRECTORY, f"{STATE_FILE_NAME}.tmp")

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
            last_line = f.readlines()[-1]
            return last_line.startswith(CHECKPOINT)

    def __remove_log(self):
        try:
            os.remove(self._log_file_path)
        except FileNotFoundError:
            pass

    def __load_state_with_log(self):
        if self.__state_exists():
            if self.__last_line_is_checkpoint():
                self.__load_from_checkpoint()
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

        if self.__tmp_state_exists():
            os.rename(self._tmp_state_file_path, self._state_file_path)
            self.__load_from_state()   

    def __replay_valid_lines(self):
        with open(self._log_file_path, "r") as log_file, open(self._truncated_log_file_path, "w") as truncate_log_file:
            for line in log_file:
                msg_size, msg = line.split(" ", 1)
                if int(msg_size) == len(msg):
                    self._component.replay(msg)
                    truncate_log_file.write(line + "\n")

            truncate_log_file.flush()

        os.rename(self._truncated_log_file_path, self._log_file_path)

    def save_state(self, new_msg: bytes):
        # Append the new message to the log
        new_msg_size = len(new_msg)
        self._log_file.write(f"{new_msg_size} {new_msg}\n")
        self._log_file.flush()

        # This could be done in an exact manner, keeping a counter in memory
        # remembering to do a checkpoint on startup (so we don't lose it on crash)
        do_checkpoint = CHANCE_OF_CHECKPOINT > random.random()
        if not do_checkpoint:
            return
        
        # get the updated state from the component
        state = self._component.get_state()

        # write the state to a tmp file
        with open(self._tmp_state_file_path, "wb") as f:
            f.write(state)

        # write the checkpoint to the log
        with open(self._log_file_path, "a") as f:
            f.write(f"{CHECKPOINT}\n")

        # rename the tmp state file to the state file
        os.rename(self._tmp_state_file_path, self._state_file_path)

        # remove the log file
        self.__remove_log()
