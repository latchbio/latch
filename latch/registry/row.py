from dataclasses import dataclass


@dataclass(frozen=True)
class Row:
    id: str
    name: str
