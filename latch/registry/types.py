from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Union

from typing_extensions import TypeAlias

from latch.registry.record import Record
from latch.registry.upstream_types.values import EmptyCell
from latch.types import LatchDir, LatchFile


@dataclass(frozen=True)
class InvalidValue:
    raw_value: str


RegistryPythonValue: TypeAlias = Union[
    str,
    datetime,
    date,
    int,
    float,
    Record,
    None,
    List["RegistryPythonValue"],
    LatchFile,
    LatchDir,
]

RecordValue: TypeAlias = Union[RegistryPythonValue, EmptyCell, InvalidValue]
