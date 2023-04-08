from dataclasses import dataclass
from typing import ClassVar, List, Literal, Optional, TypedDict, Union

from typing_extensions import Self, TypeAlias


class InvalidValue(TypedDict):
    rawValue: str
    valid: Literal[False]


class PrimitiveStringValueValid(TypedDict):
    value: str
    valid: Literal[True]


PrimitiveStringValue: TypeAlias = Union[PrimitiveStringValueValid, InvalidValue]


class PrimitiveNumberValueValid(TypedDict):
    value: float
    valid: Literal[True]


PrimitiveNumberValue: TypeAlias = Union[PrimitiveNumberValueValid, InvalidValue]


class PrimitiveNullValueValid(TypedDict):
    value: None
    valid: Literal[True]


PrimitiveNullValue: TypeAlias = Union[PrimitiveNullValueValid, InvalidValue]


class PrimitiveBooleanValueValid(TypedDict):
    value: bool
    valid: Literal[True]


PrimitiveBooleanValue: TypeAlias = Union[PrimitiveBooleanValueValid, InvalidValue]


class BlobValue(TypedDict):
    ldataNodeId: str


class PrimitiveBlobValueValid(TypedDict):
    value: BlobValue
    valid: Literal[True]


PrimitiveBlobValue: TypeAlias = Union[PrimitiveBlobValueValid, InvalidValue]


class LinkValue(TypedDict):
    sampleId: str


class PrimitiveLinkValueValid(TypedDict):
    value: LinkValue
    valid: Literal[True]


PrimitiveLinkValue: TypeAlias = Union[PrimitiveLinkValueValid, InvalidValue]


class PrimitiveEnumValueValid(TypedDict):
    value: str
    valid: Literal[True]


PrimitiveEnumValue: TypeAlias = Union[PrimitiveEnumValueValid, InvalidValue]

PrimitiveValue: TypeAlias = Union[
    PrimitiveStringValue,
    PrimitiveNumberValue,
    PrimitiveNullValue,
    PrimitiveBlobValue,
    PrimitiveLinkValue,
    PrimitiveEnumValue,
    PrimitiveBooleanValue,
]

ArrayValue: TypeAlias = List[PrimitiveValue]


class UnionValue(TypedDict):
    tag: str
    value: "DBValue"


DBValue: TypeAlias = Union[PrimitiveValue, ArrayValue, UnionValue]


@dataclass(frozen=True)
class EmptyCell:
    _singleton: ClassVar[Optional["EmptyCell"]] = None

    def __new__(cls) -> Self:
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)

        return cls._singleton


Value: TypeAlias = Union[DBValue, EmptyCell]
