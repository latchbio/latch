from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Generic, List, Literal, Tuple, Type, TypeVar, Union

from typing_extensions import TypeAlias

from latch.registry.record import Record
from latch.registry.upstream_types.types import DBType
from latch.registry.upstream_types.values import EmptyCell
from latch.types.directory import LatchDir
from latch.types.file import LatchFile

if not TYPE_CHECKING:
    try:
        from enum import StrEnum
    except ImportError:

        class StrEnum(str, Enum):
            ...

else:

    class StrEnum(str, Enum):
        ...


@dataclass(frozen=True)
class InvalidValue:
    """Registry :class:`Record` value that failed validation."""

    raw_value: str
    """User-provided string representation of the invalid value.

    May be `""` (the empty string) if the value is missing but the column is required.
    """


RegistryPrimitivePythonValue: TypeAlias = Union[
    str,
    datetime,
    date,
    int,
    float,
    LatchFile,
    LatchDir,
    None,
    bool,
]

RegistryPythonValue: TypeAlias = Union[
    RegistryPrimitivePythonValue,
    Record,
    List["RegistryPythonValue"],
]

RecordValue: TypeAlias = Union[RegistryPythonValue, EmptyCell, InvalidValue]


@dataclass(frozen=True)
class Column:
    """Registry :class:`Table` column definition.

    :meth:`Table.get_columns` is the typical way to get a :class:`Column`.
    """

    key: str
    """Unique identifier within the table. Not globally unique."""
    type: Union[Type[RegistryPythonValue], Type[Union[RegistryPythonValue, EmptyCell]]]
    """Python equivalent of the stored column type."""
    # fixme(maximsmol): deal with defaults
    # default: Union[RegistryPythonValue, EmptyCell]
    upstream_type: DBType
    """Raw column type.

    Used to convert between Python values and Registry values.
    """


RegistryEnumDefinitionArg = TypeVar("RegistryEnumDefinitionArg", bound=Tuple[str, ...])


class RegistryEnumDefinition(Generic[RegistryEnumDefinitionArg]):
    def __new__(cls, *values: str):
        return RegistryEnumDefinition[Tuple[tuple(Literal[v] for v in values)]]


LinkedRecordTypeArg = TypeVar("LinkedRecordTypeArg", bound=str)


class LinkedRecordType(Generic[LinkedRecordTypeArg]):
    def __new__(cls, id: str):
        return LinkedRecordType[Literal[id]]


RegistryPrimitivePythonType: TypeAlias = Union[
    Type[str],
    Type[int],
    Type[float],
    Type[date],
    Type[datetime],
    Type[bool],
    Type[LatchFile],
    Type[LatchDir],
    Type[StrEnum],
    Type[RegistryEnumDefinition],
    Type[LinkedRecordType],
]
RegistryPythonType: TypeAlias = Union[
    RegistryPrimitivePythonType,
    Type[List[LatchFile]],
    Type[List[LatchDir]],
    Type[List[LinkedRecordType]],
]
