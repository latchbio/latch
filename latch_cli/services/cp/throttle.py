from multiprocessing.managers import BaseManager
from typing import Type

from attr import dataclass


@dataclass
class Throttle:
    delay: float = 0

    def get_delay(self):
        return self.delay

    def set_delay(self, d: float):
        self.delay = d
