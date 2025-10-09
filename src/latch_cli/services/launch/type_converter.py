import dataclasses
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from flytekit.models import literals as _literals
from flytekit.models import types as _types
from flytekit.models.core import types as _core_types
from google.protobuf import struct_pb2

from latch.ldata.path import LPath
from latch.types.directory import LatchDir
from latch.types.file import LatchFile


def _convert_python_value_to_literal(
    value: object,
    flyte_literal_type: _types.LiteralType,
) -> _literals.Literal:
    if flyte_literal_type.union_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_union(value, flyte_literal_type)

    if value is None:
        return _create_none_literal()

    if flyte_literal_type.simple is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_primitive(value, flyte_literal_type.simple)

    if flyte_literal_type.collection_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_collection(value, flyte_literal_type.collection_type)

    if flyte_literal_type.map_value_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_map(value, flyte_literal_type.map_value_type)

    if flyte_literal_type.record_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_record(value, flyte_literal_type.record_type)

    if flyte_literal_type.blob is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_blob(value, flyte_literal_type.blob)

    if flyte_literal_type.enum_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_enum(value, flyte_literal_type.enum_type)

    raise ValueError(f"Unsupported literal type: {flyte_literal_type}")


def _create_none_literal() -> _literals.Literal:
    return _literals.Literal(
        scalar=_literals.Scalar(
            none_type=_literals.Void(),
        )
    )


def _convert_primitive(
    value: object, simple_type: _types.SimpleType
) -> _literals.Literal:
    if simple_type == _types.SimpleType.STRUCT:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_to_struct(value)

    primitive: _literals.Primitive | None = None

    if simple_type == _types.SimpleType.INTEGER:  # pyright: ignore[reportUnnecessaryComparison]
        if isinstance(value, Enum):
            primitive = _literals.Primitive(integer=int(value.value))
        else:
            primitive = _literals.Primitive(integer=int(value))

    elif simple_type == _types.SimpleType.FLOAT:  # pyright: ignore[reportUnnecessaryComparison]
        primitive = _literals.Primitive(float_value=float(value))

    elif simple_type == _types.SimpleType.STRING:  # pyright: ignore[reportUnnecessaryComparison]
        if isinstance(value, Enum):
            primitive = _literals.Primitive(string_value=str(value.value))
        else:
            primitive = _literals.Primitive(string_value=str(value))

    elif simple_type == _types.SimpleType.BOOLEAN:  # pyright: ignore[reportUnnecessaryComparison]
        primitive = _literals.Primitive(boolean=bool(value))

    elif simple_type == _types.SimpleType.DATETIME:  # pyright: ignore[reportUnnecessaryComparison]
        if isinstance(value, datetime):
            primitive = _literals.Primitive(datetime=value)
        elif isinstance(value, str):
            dt = datetime.fromisoformat(value)
            primitive = _literals.Primitive(datetime=dt)
        else:
            raise ValueError(f"Cannot convert {value} to datetime")

    elif simple_type == _types.SimpleType.DURATION:  # pyright: ignore[reportUnnecessaryComparison]
        if isinstance(value, timedelta):
            primitive = _literals.Primitive(duration=value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            primitive = _literals.Primitive(duration=timedelta(seconds=value))
        else:
            raise ValueError(f"Cannot convert {value} to duration")

    elif simple_type == _types.SimpleType.NONE:  # pyright: ignore[reportUnnecessaryComparison]
        if value is None:
            return _create_none_literal()
        raise ValueError("Expected None for NONE type")

    else:
        raise ValueError(f"Unsupported simple type: {simple_type}")

    return _literals.Literal(scalar=_literals.Scalar(primitive=primitive))


def _to_public_fields(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value  # type: ignore[return-value]
    if is_dataclass(value):
        return asdict(value)  # pyright: ignore[reportArgumentType]
    if hasattr(value, "__dict__"):
        return {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
    raise ValueError(f"Cannot extract fields from {type(value)}")


def _convert_to_struct(value: object) -> _literals.Literal:
    fields = _to_public_fields(value)

    struct_value = struct_pb2.Struct()
    for field_name, field_value in fields.items():
        struct_value.fields[field_name].CopyFrom(
            _python_value_to_struct_value(field_value)
        )

    return _literals.Literal(
        scalar=_literals.Scalar(generic=struct_value)
    )


def _python_value_to_struct_value(value: object) -> struct_pb2.Value:
    if value is None:
        return struct_pb2.Value(null_value=struct_pb2.NullValue.NULL_VALUE)

    if isinstance(value, bool):
        return struct_pb2.Value(bool_value=value)

    if isinstance(value, (int, float)):
        return struct_pb2.Value(number_value=float(value))

    if isinstance(value, str):
        return struct_pb2.Value(string_value=value)

    if isinstance(value, Enum):
        return _python_value_to_struct_value(value.value)

    if isinstance(value, (list, tuple)):
        list_value = struct_pb2.ListValue()
        for item in value:
            list_value.values.append(_python_value_to_struct_value(item))
        return struct_pb2.Value(list_value=list_value)

    if isinstance(value, dict):
        struct_value = struct_pb2.Struct()
        for k, v in value.items():
            struct_value.fields[str(k)].CopyFrom(_python_value_to_struct_value(v))
        return struct_pb2.Value(struct_value=struct_value)

    if is_dataclass(value) or hasattr(value, "__dict__"):
        obj_dict = _to_public_fields(value)
        return _python_value_to_struct_value(obj_dict)

    return struct_pb2.Value(string_value=str(value))


def _convert_collection(
    value: object,
    element_type: _types.LiteralType,
) -> _literals.Literal:
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"Expected list or tuple, got {type(value)}")

    literals = [
        _convert_python_value_to_literal(item, element_type)
        for item in value
    ]

    return _literals.Literal(
        collection=_literals.LiteralCollection(literals=literals)
    )


def _convert_map(
    value: object,
    value_type: _types.LiteralType,
) -> _literals.Literal:
    if not isinstance(value, dict):
        raise TypeError(f"Expected dict, got {type(value)}")

    literals = {
        str(k): _convert_python_value_to_literal(v, value_type)
        for k, v in value.items()
    }

    return _literals.Literal(
        map=_literals.LiteralMap(literals=literals)
    )


def _convert_record(
    value: object,
    record_type: _types.RecordType,
) -> _literals.Literal:
    if isinstance(value, dict):
        src = value
    elif is_dataclass(value):
        src = {f.name: getattr(value, f.name) for f in dataclasses.fields(value)}
    # note(aidan): removed because it caused classes (Like LatchFile) to match into records
    # elif hasattr(value, "__dict__"):
    #     src = {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
    else:
        raise ValueError(f"Cannot convert {type(value)} to record")

    record_fields: list[_literals.RecordField] = []
    for field in record_type.fields:
        key = field.key
        sub_type = field.literal_type

        if key in src:
            sub_value = src[key]
        else:
            ut = getattr(sub_type, "union_type", None)
            if ut is None or not any(
                v.simple == _types.SimpleType.NONE for v in ut.variants  # pyright: ignore[reportUnnecessaryComparison]
            ):
                raise ValueError(
                    f"Record field '{key}' is required but missing for value of type {type(value)}"
                )

            sub_value = None

        sub_literal = _convert_python_value_to_literal(sub_value, sub_type)
        record_fields.append(_literals.RecordField(key=key, value=sub_literal))

    return _literals.Literal(record=_literals.Record(fields=record_fields))


def _convert_blob(
    value: object,
    blob_type: _core_types.BlobType,
) -> _literals.Literal:

    remote_uri: str | None = None
    if isinstance(value, (LatchFile, LatchDir)):
        remote_uri = value.remote_path if value.remote_path is not None else value.path
    elif isinstance(value, LPath):
        remote_uri = value.path
    elif isinstance(value, str):
        remote_uri = value

    if remote_uri is None:
        raise TypeError(
            "Cannot convert to blob: expected a remote URI or a LatchFile/LatchDir/LPath with remote_path."
        )

    return _literals.Literal(
        scalar=_literals.Scalar(
            blob=_literals.Blob(
                metadata=_literals.BlobMetadata(type=blob_type),
                uri=remote_uri,
            )
        )
    )


def _convert_enum(
    value: object,
    enum_type: _core_types.EnumType,
) -> _literals.Literal:
    if isinstance(value, Enum):
        enum_val = value.value
    else:
        enum_val = value

    if not isinstance(enum_val, str):
        enum_val = str(enum_val)

    if enum_type.values and enum_val not in enum_type.values:
        raise ValueError(
            f"'{enum_val}' is not a valid value for enum {enum_type.values}"
        )

    prim = _literals.Primitive(string_value=enum_val)
    return _literals.Literal(scalar=_literals.Scalar(primitive=prim))


def _convert_union(
    value: object,
    union_type: _types.LiteralType,
) -> _literals.Literal:
    if value is None:
        variants = union_type.union_type.variants if union_type.union_type else []
        if not any(v.simple == _types.SimpleType.NONE for v in variants):  # pyright: ignore[reportUnnecessaryComparison]
            raise ValueError("Union does not allow NONE variant")

        none_literal = _create_none_literal()
        return _literals.Literal(
            scalar=_literals.Scalar(
                union=_literals.Union(
                    value=none_literal,
                    stored_type=_types.LiteralType(
                        simple=_types.SimpleType.NONE,
                        structure=_types.TypeStructure(tag="none"),
                    ),
                )
            )
        )

    variants = union_type.union_type.variants if union_type.union_type else []

    errors = []
    # does not support tagging values / overlapping variants where one is intended
    possible_variants = []
    for variant in variants:
        try:
            converted = _convert_python_value_to_literal(value, variant)
            stored = _types.LiteralType(
                simple=variant.simple,
                collection_type=variant.collection_type,
                map_value_type=variant.map_value_type,
                record_type=variant.record_type,
                blob=variant.blob,
                enum_type=variant.enum_type,
                union_type=variant.union_type,
                metadata=variant.metadata,
                structure=variant.structure,
                annotation=variant.annotation,
            )
            possible_variants.append(_literals.Literal(
                scalar=_literals.Scalar(
                    union=_literals.Union(
                        value=converted,
                        stored_type=stored,
                    )
                )
            ))
        except Exception as e:
            errors.append((variant, str(e)))
            continue

    if len(possible_variants) == 1:
        return possible_variants[0]

    if len(possible_variants) > 1:
        raise ValueError(f"Multiple possible union variants for value\n{value}\n{', '.join([str(v.scalar.union.stored_type) for v in possible_variants])}")

    error_msg = "Could not convert value to any union variant. Tried:\n"
    for variant, error in errors:
        error_msg += f"  - {variant}: {error}\n"
    raise ValueError(error_msg)


def convert_inputs_to_literals(
    params: dict[str, object],
    flyte_interface_types: dict[str, Any],  # Variable map
) -> dict[str, _literals.Literal]:
    result: dict[str, _literals.Literal] = {}

    for param_name, param_value in params.items():
        if param_name not in flyte_interface_types:
            raise ValueError(f"Parameter '{param_name}' not found in workflow interface")

        variable = flyte_interface_types[param_name]
        literal_type = variable.type

        result[param_name] = _convert_python_value_to_literal(
            param_value,
            literal_type,
        )

    return result
