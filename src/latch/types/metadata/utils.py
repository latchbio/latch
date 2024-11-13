from __future__ import annotations

from collections.abc import Collection
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Protocol, TypeVar, Union

from typing_extensions import TypeAlias

from ..directory import LatchDir
from ..file import LatchFile

if TYPE_CHECKING:
    from dataclasses import Field


# https://stackoverflow.com/questions/54668000/type-hint-for-an-instance-of-a-non-specific-dataclass
class _IsDataclass(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Field[Any]]]


DC = TypeVar("DC", bound=_IsDataclass)

ParameterType: TypeAlias = Union[
    None,
    int,
    float,
    str,
    bool,
    LatchFile,
    LatchDir,
    Enum,
    _IsDataclass,
    Collection["ParameterType"],
]

P = TypeVar("P", bound=ParameterType)
