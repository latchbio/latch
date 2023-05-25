from dataclasses import dataclass
from enum import Enum

from latch_cli.constants import latch_constants


class Progress(Enum):
    none = "none"
    total = "total"
    tasks = "tasks"


@dataclass(frozen=True)
class CPConfig:
    max_concurrent_files: int
    progress: Progress
    verbose: bool

    chunk_size: int
