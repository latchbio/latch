from dataclasses import dataclass


@dataclass
class Throttle:
    delay: float = 0

    def get_delay(self):
        return self.delay

    def set_delay(self, d: float):
        self.delay = d
