try:
    from typing import get_args, get_origin
except ImportError:
    from typing_extensions import get_args, get_origin

import dataclasses
import enum
import typing

import google.protobuf.json_format as gpjson
from dataclasses_json import dataclass_json
from flyteidl.core.literals_pb2 import Literal as _Literal
from flyteidl.core.types_pb2 import LiteralType as _LiteralType
from flytekit.models.types import LiteralType

from latch.types import LatchDir, LatchFile


class _Unsupported:
    ...


_simple_table = {
    0: type(None),
    1: int,
    2: float,
    3: str,
    4: bool,
    5: _Unsupported,
    6: _Unsupported,
    7: _Unsupported,
    8: _Unsupported,
    9: object,
}

_primitive_table = {
    type(None): None,
    int: 0,
    float: 0.0,
    str: "foo",
    bool: False,
}

_enum_counter = 0

#
# JSON schema parsing utilities for @dataclass_json.
#


def _get_schema_name(metadata: dict) -> str:
    return metadata["$ref"].split("/")[-1]


def _guess_property_python_type(property_val: dict, json_schema: dict) -> str:
    """Construct type for python class field represented in JSON schema."""

    if "oneOf" in property_val:

        # All field types in LiteralType representations of
        # (Dataclass JSON) python classes borrow the JSON
        # schema representation of their type as generated in python.
        #
        # @dataclass_json
        # @dataclass
        # class Foo
        #   a: int
        #   b: List[str]
        #
        # >>  Foo(a=1, b=["bar"]).schema()
        #
        # Member fields use the typing outlined [here](
        # https://json-schema.org/understanding-json-schema/reference/type.html)
        # and python types are inferred directly from these,
        # skipping IDL representations completely.
        #
        # The one exception is @maximsmol's implementation of
        # Union with (Dataclass JSON) python class fields, which
        # will show *IDL* representations of Union variants in the
        # "const" schema property:

        # "oneOf": [
        #   { "properties":
        #     { "type":
        #       { "const":
        #         ...FlyteLiteralType JSON
        #       }
        #     }
        #   },
        # ]
        #
        # Logic used to handle Union fields is different from all
        # other field types.

        variants = []
        for x in property_val["oneOf"]:
            literal_type = gpjson.ParseDict(
                x["properties"]["type"]["const"], _LiteralType()
            )
            variants.append(
                guess_python_type(
                    LiteralType.from_flyte_idl(literal_type),
                )
            )

        return typing.Union[tuple(variants)]

    property_type = property_val["type"]
    if property_type == "object":
        if property_val.get("$ref"):
            nested_name = _get_schema_name(property_val)
            # LatchFile and LatchDir are serialized as type "object" in JSON
            # schema indistinguishable from eg. nested python classes.
            if nested_name == "LatchfileSchema":
                return LatchFile
            elif nested_name == "LatchdirSchema":
                return LatchDir
            return _construct_class_from_json_schema(json_schema, nested_name)
        elif property_val.get("additionalProperties"):
            return typing.Dict[
                str,
                _guess_property_python_type(
                    property_val["additionalProperties"], json_schema
                ),
            ]
        else:
            return typing.Dict[
                str, _guess_property_python_type(property_val, json_schema)
            ]
    elif property_val["type"] == "array":
        return typing.List[
            _guess_property_python_type(property_val["items"], json_schema)
        ]

    elif property_type == "string":
        return str
    elif property_type == "integer":
        return int
    elif property_type == "boolean":
        return bool
    elif property_type == "number":
        element_format = property_val["format"] if "format" in property_val else None
        if element_format == "integer":
            return int
        else:
            return float
    return str


def _construct_class_from_json_schema(json_schema: dict, schema_name: str):

    attribute_list = []
    for property_key, property_val in json_schema[schema_name]["properties"].items():
        attribute_list.append(
            (
                property_key,
                _guess_property_python_type(property_val, json_schema),
            )
        )

    _dataclass = dataclass_json(dataclasses.make_dataclass(schema_name, attribute_list))
    # Flag for code generator (LatchFile/LatchDir would also be recognized
    # under naive is_dataclass(t) check)
    _dataclass._from_dataclass = True
    return _dataclass


def _guess_python_class(literal_type: LiteralType):

    # Dataclass JSON implementation
    if literal_type.metadata is not None and "definitions" in literal_type.metadata:
        schema_name = _get_schema_name(literal_type.metadata)
        return _construct_class_from_json_schema(
            literal_type.metadata["definitions"], schema_name
        )
    # TODO: Record implementation

    raise ValueError(
        f"Unable to construct a python type for literal type: {literal_type}"
    )


###


def guess_python_type(literal_type: LiteralType):
    """Transform a LiteralType to a Python type."""

    if literal_type.simple is not None:
        if literal_type.simple == 9:
            return _guess_python_class(literal_type)
        return _simple_table[literal_type.simple]

    if literal_type.collection_type is not None:
        return typing.List[guess_python_type(literal_type.collection_type)]

    if literal_type.blob is not None:

        dim = literal_type.blob.dimensionality
        if dim == 0:
            return LatchFile
        else:
            return LatchDir

    if literal_type.union_type is not None:

        variant_types = [
            guess_python_type(variant) for variant in literal_type.union_type.variants
        ]

        unique_variants = []
        for t in variant_types:
            if t not in unique_variants:
                unique_variants.append(t)

        return typing.Union[tuple(unique_variants)]

    if literal_type.enum_type is not None:

        escaped_variant_names = list(
            map(
                lambda x: "".join(
                    filter(str.isidentifier, x.lower().replace(" ", "_"))
                ),
                literal_type.enum_type.values,
            )
        )

        # Construct a unique symbol to represent each enum as a python class.
        global _enum_counter
        python_enum = enum.Enum(
            f"enum{_enum_counter}", {x: x for x in escaped_variant_names}
        )
        _enum_counter += 1

        return python_enum

    raise NotImplementedError(
        f"The flyte literal_type {literal_type} cannot be transformed to a python type."
    )


def best_effort_python_val(t: typing.T):
    """Produce a "best-effort" default value given a python type.

    Unlike `guess_python_val` there is no Literal value acting as a guide.
    """

    if t in _primitive_table:
        return _primitive_table[t]

    if t is list:
        return []

    file_like_table = {
        LatchDir: LatchDir("latch:///foobar"),
        LatchFile: LatchFile("latch:///foobar"),
    }
    if t in file_like_table:
        return file_like_table[t]

    if type(t) is enum.EnumMeta:
        return list(t.__members__.values())[0]

    if get_origin(t) is list:
        list_args = get_args(t)
        if len(list_args) == 0:
            return []
        return [best_effort_python_val(arg) for arg in list_args]

    if get_origin(t) is typing.Union:
        return best_effort_python_val(get_args(t)[0])

    if get_origin(t) is None:

        if "__dataclass_fields__" in t.__dict__:
            fields = t.__dict__["__dataclass_fields__"]
            dataclass_args = {
                k: best_effort_python_val(v.type) for k, v in fields.items()
            }
            return t(**dataclass_args)

        # TODO : record implementation

    raise NotImplementedError(
        f"Unable to produce a best-effort value for the python type {t}"
    )


def guess_python_val(literal: _Literal, python_type: typing.T):
    """Transform flyte literal value to native python value."""

    if literal.scalar is not None:
        if literal.scalar.none_type is not None:
            return None

        if literal.scalar.primitive is not None:
            primitive = literal.scalar.primitive

            if primitive.string_value is not None:
                if type(python_type) is enum.EnumMeta:
                    return list(python_type.__members__.values())[0]
                return str(primitive.string_value)

            if primitive.integer is not None:
                return int(primitive.integer)
            if primitive.float_value is not None:
                return float(primitive.float_value)
            if primitive.boolean is not None:
                return bool(primitive.boolean)

        if literal.scalar.blob is not None:
            blob = literal.scalar.blob
            dim = blob.metadata.type.dimensionality
            if dim == 0:
                return LatchFile(blob.uri)
            else:
                return LatchDir(blob.uri)

    # collection
    if literal.collection is not None:
        p_list = []
        for item in literal.collection.literals:
            p_list.append(guess_python_val(item, get_args(python_type)[0]))
        return p_list

    # sum: TODO

    # enum: TODO

    # class: TODO

    raise NotImplementedError(
        f"The flyte literal {literal} cannot be transformed to a python type."
    )


def build_python_literal(python_val: any, python_type: typing.T) -> (str, str):
    """Construct literal python value and human-readable type.

    The returned python value is escaped so that when formatted into a code block it
    is correct python code.

    The returned human-readable type is to be used in eg. a comment to provide
    information about the original type of the value.

    ```
    >> build_python_literal("foo", type("foo")
    ('"foo"', <class 'str'>)
    ```

    """

    if python_type is str:
        return f'"{python_val}"', python_type.__name__

    if type(python_type) is enum.EnumMeta:
        return python_val, python_type

    if get_origin(python_type) is typing.Union:
        variants = get_args(python_type)
        type_repr = "typing.Union["
        for i, variant in enumerate(variants):
            if i < len(variants) - 1:
                delimiter = ", "
            else:
                delimiter = ""
            variant_val, variant_type_repr = build_python_literal(python_val, variant)
            if type(python_val) is variant:
                python_val = variant_val
            type_repr += f"{variant_type_repr}{delimiter}"
        type_repr += "]"
        return python_val, type_repr

    if get_origin(python_type) is list:
        collection_literal = "["
        if len(python_val) > 0:
            for i, item in enumerate(python_val):
                item_val, item_type = build_python_literal(
                    item, get_args(python_type)[0]
                )
                if i < len(python_val) - 1:
                    delimiter = ","
                else:
                    delimiter = ""
                collection_literal += f"{item_val}{delimiter}"
        else:
            list_t = get_args(python_type)[0]
            _, item_type = build_python_literal(best_effort_python_val(list_t), list_t)
        collection_literal += "]"
        return collection_literal, f"typing.List[{item_type}]"

    return python_val, python_type
