from dataclasses import dataclass
from enum import Enum


class Progress(Enum):
    none = "none"
    total = "total"
    tasks = "tasks"


@dataclass(frozen=True)
class CPConfig:
    progress: Progress
    verbose: bool
