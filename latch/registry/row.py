from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    id: str
    name: str
