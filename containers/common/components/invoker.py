import time
from typing import Callable


class Invoker:
    def __init__(self, time_before_invocation: int, callback: Callable):
        self._time_before_invocation = time_before_invocation
        self._callback = callback
        self._last_invocation = None

    def check(self):
        if self._last_invocation is None:
            self._last_invocation = time.time()
            self._callback()
        elif time.time() - self._last_invocation > self._time_before_invocation:
            self._last_invocation = time.time()
            self._callback()
