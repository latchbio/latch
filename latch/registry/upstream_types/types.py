from typing import Dict, List, Literal, TypedDict, Union

from typing_extensions import NotRequired, TypeAlias

from latch.types.json import JsonObject


class PrimitiveTypeBasic(TypedDict):
    primitive: Union[
        Literal["string"],
        Literal["integer"],
        Literal["number"],
        Literal["datetime"],
        Literal["blob"],
        Literal["date"],
        Literal["null"],
        Literal["boolean"],
    ]
    metadata: NotRequired[JsonObject]


class PrimitiveTypeLink(TypedDict):
    primitive: Literal["link"]
    experimentId: str
    metadata: NotRequired[JsonObject]


class PrimitiveTypeEnum(TypedDict):
    primitive: Literal["enum"]
    members: List[str]
    metadata: NotRequired[JsonObject]


PrimitiveType: TypeAlias = Union[
    PrimitiveTypeBasic, PrimitiveTypeLink, PrimitiveTypeEnum
]

PrimitiveTypeName = Union[
    Literal["string"],
    Literal["integer"],
    Literal["number"],
    Literal["datetime"],
    Literal["blob"],
    Literal["date"],
    Literal["null"],
    Literal["boolean"],
    Literal["link"],
    Literal["enum"],
]


class UnionType(TypedDict):
    union: Dict[str, "RegistryType"]


class ArrayType(TypedDict):
    array: "RegistryType"


RegistryType: TypeAlias = Union[PrimitiveType, UnionType, ArrayType]


class DBType(TypedDict):
    type: RegistryType
    allowEmpty: bool
