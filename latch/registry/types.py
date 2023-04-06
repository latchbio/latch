from dataclasses import dataclass
from typing import Dict, List, Literal, TypedDict, Union

from typing_extensions import NotRequired

JsonValue = Union[
    Union[str, bool, int, float, None],
    Dict[str, "JsonValue"],
    List["JsonValue"],
]


@dataclass(frozen=True)
class EmptyCell:
    ...


registry_empty_cell = EmptyCell()


class RegistryPrimitiveSimpleType(TypedDict):
    primitive: Literal[
        "string",
        "integer",
        "number",
        "datetime",
        "blob",
        "date",
        "null",
        "boolean",
    ]
    metadata: NotRequired[Dict[str, JsonValue]]


class RegistryPrimitiveLinkType(TypedDict):
    primitive: Literal["link"]
    experimentId: str
    metadata: NotRequired[Dict[str, JsonValue]]


class RegistryPrimitiveEnumType(TypedDict):
    primitive: Literal["enum"]
    members: List[str]
    metadata: NotRequired[Dict[str, JsonValue]]


RegistryPrimitiveType = Union[
    RegistryPrimitiveSimpleType,
    RegistryPrimitiveLinkType,
    RegistryPrimitiveEnumType,
]


class RegistryArrayType(TypedDict):
    array: "RegistryType"


class RegistryUnionType(TypedDict):
    union: Dict[str, "RegistryType"]


RegistryType = Union[
    RegistryPrimitiveType,
    RegistryArrayType,
    RegistryUnionType,
]


# for actual use as a python value
@dataclass(frozen=True)
class InvalidValue:
    raw_value: str


# for type hints
class RegistryInvalidValue(TypedDict):
    valid: Literal[False]
    rawValue: str


class RegistryPrimitiveSimpleValue(TypedDict):
    valid: Literal[True]
    value: Union[str, int, float, bool, None]


class LinkValue(TypedDict):
    sampleId: str


class RegistryPrimitiveLinkValue(TypedDict):
    valid: Literal[True]
    value: LinkValue


class BlobValue(TypedDict):
    ldataNodeId: str


class RegistryPrimitiveBlobValue(TypedDict):
    valid: Literal[True]
    value: BlobValue


RegistryPrimitiveValue = Union[
    RegistryPrimitiveSimpleValue,
    RegistryPrimitiveLinkValue,
    RegistryPrimitiveBlobValue,
    RegistryInvalidValue,
]

RegistryArrayValue = List["RegistryDBValue"]


class RegistryUnionValue(TypedDict):
    tag: str
    value: "RegistryDBValue"


RegistryDBValue = Union[
    RegistryPrimitiveValue,
    RegistryArrayValue,
    RegistryUnionValue,
]
