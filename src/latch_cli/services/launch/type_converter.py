"""Type converter for converting Python values to Flyte Literals without requiring exact type matches.

This module provides best-effort conversion that works with structurally compatible values,
allowing dataclasses, enums, and other types to be passed without exact module import matches.
"""

import dataclasses
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from flytekit.core.context_manager import FlyteContext
from flytekit.models import literals as _literals
from flytekit.models import types as _types
from flytekit.models.core import types as _core_types
from google.protobuf import struct_pb2


def convert_python_value_to_literal(
    value: Any,  # noqa: ANN401
    flyte_literal_type: _types.LiteralType,
    ctx: FlyteContext,
) -> _literals.Literal:
    if flyte_literal_type.union_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_union(value, flyte_literal_type, ctx)

    if value is None:
        return _create_none_literal()

    if flyte_literal_type.simple is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_primitive(value, flyte_literal_type.simple)

    if flyte_literal_type.collection_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_collection(value, flyte_literal_type.collection_type, ctx)

    if flyte_literal_type.map_value_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_map(value, flyte_literal_type.map_value_type, ctx)

    if flyte_literal_type.record_type is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_record(value, flyte_literal_type.record_type, ctx)

    if flyte_literal_type.blob is not None:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_blob(value, flyte_literal_type.blob, ctx)

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
    value: Any, simple_type: _types.SimpleType  # noqa: ANN401
) -> _literals.Literal:
    if simple_type == _types.SimpleType.STRUCT:  # pyright: ignore[reportUnnecessaryComparison]
        return _convert_to_struct(value)

    primitive = None

    if simple_type == _types.SimpleType.INTEGER:  # pyright: ignore[reportUnnecessaryComparison]
        if isinstance(value, Enum):
            primitive = _literals.Primitive(integer=int(value.value))
        else:
            primitive = _literals.Primitive(integer=int(value))

    elif simple_type == _types.SimpleType.FLOAT:  # pyright: ignore[reportUnnecessaryComparison]
        primitive = _literals.Primitive(float_value=float(value))

    elif simple_type == _types.SimpleType.STRING:
        # Handle enums that have string values
        if isinstance(value, Enum):
            primitive = _literals.Primitive(string_value=str(value.value))
        else:
            primitive = _literals.Primitive(string_value=str(value))
            
    elif simple_type == _types.SimpleType.BOOLEAN:
        primitive = _literals.Primitive(boolean=bool(value))
        
    elif simple_type == _types.SimpleType.DATETIME:
        if isinstance(value, datetime):
            primitive = _literals.Primitive(datetime=value)
        else:
            # Try to parse as datetime
            if isinstance(value, str):
                dt = datetime.fromisoformat(value)
                primitive = _literals.Primitive(datetime=dt)
            else:
                raise ValueError(f"Cannot convert {value} to datetime")
                
    elif simple_type == _types.SimpleType.DURATION:
        if isinstance(value, timedelta):
            primitive = _literals.Primitive(duration=value)
        else:
            # Try to convert to timedelta
            if isinstance(value, (int, float)):
                primitive = _literals.Primitive(duration=timedelta(seconds=value))
            else:
                raise ValueError(f"Cannot convert {value} to duration")
    
    elif simple_type == _types.SimpleType.NONE:
        return _create_none_literal()
    
    else:
        raise ValueError(f"Unsupported simple type: {simple_type}")
    
    return _literals.Literal(scalar=_literals.Scalar(primitive=primitive))


def _convert_to_struct(value: Any) -> _literals.Literal:
    """
    Convert a Python object to a Flyte struct literal.
    
    This works with dicts, dataclasses, or any object with attributes.
    """
    # Extract fields from the value
    if isinstance(value, dict):
        fields = value
    elif is_dataclass(value):
        fields = asdict(value)
    elif hasattr(value, "__dict__"):
        # Duck typing - extract public attributes
        fields = {
            k: v
            for k, v in value.__dict__.items()
            if not k.startswith("_")
        }
    else:
        raise ValueError(f"Cannot convert {type(value)} to struct")
    
    # Convert to protobuf Struct
    struct_value = struct_pb2.Struct()
    
    for field_name, field_value in fields.items():
        struct_value.fields[field_name].CopyFrom(
            _python_value_to_struct_value(field_value)
        )
    
    return _literals.Literal(
        scalar=_literals.Scalar(generic=struct_value)
    )


def _python_value_to_struct_value(value: Any) -> struct_pb2.Value:
    """Convert a Python value to a protobuf Value for use in Struct."""
    if value is None:
        return struct_pb2.Value(null_value=struct_pb2.NullValue.NULL_VALUE)
    
    elif isinstance(value, bool):
        # Must check bool before int since bool is a subclass of int
        return struct_pb2.Value(bool_value=value)
    
    elif isinstance(value, (int, float)):
        return struct_pb2.Value(number_value=float(value))
    
    elif isinstance(value, str):
        return struct_pb2.Value(string_value=value)
    
    elif isinstance(value, Enum):
        # Convert enum to its value
        return _python_value_to_struct_value(value.value)
    
    elif isinstance(value, (list, tuple)):
        list_value = struct_pb2.ListValue()
        for item in value:
            list_value.values.append(_python_value_to_struct_value(item))
        return struct_pb2.Value(list_value=list_value)
    
    elif isinstance(value, dict):
        struct_value = struct_pb2.Struct()
        for k, v in value.items():
            struct_value.fields[str(k)].CopyFrom(_python_value_to_struct_value(v))
        return struct_pb2.Value(struct_value=struct_value)
    
    elif is_dataclass(value):
        # Convert dataclass to dict first
        return _python_value_to_struct_value(asdict(value))
    
    elif hasattr(value, "__dict__"):
        # Duck typing for objects
        obj_dict = {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
        return _python_value_to_struct_value(obj_dict)
    
    else:
        # Last resort: convert to string
        return struct_pb2.Value(string_value=str(value))


def _convert_collection(
    value: Any,
    element_type: _types.LiteralType,
    ctx: FlyteContext,
) -> _literals.Literal:
    """Convert a Python list/tuple to a Flyte collection literal."""
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"Expected list or tuple, got {type(value)}")
    
    literals = [
        convert_python_value_to_literal(item, element_type, ctx)
        for item in value
    ]
    
    return _literals.Literal(
        collection=_literals.LiteralCollection(literals=literals)
    )


def _convert_map(
    value: Any,
    value_type: _types.LiteralType,
    ctx: FlyteContext,
) -> _literals.Literal:
    """Convert a Python dict to a Flyte map literal."""
    if not isinstance(value, dict):
        raise ValueError(f"Expected dict, got {type(value)}")
    
    literals = {
        str(k): convert_python_value_to_literal(v, value_type, ctx)
        for k, v in value.items()
    }
    
    return _literals.Literal(
        map=_literals.LiteralMap(literals=literals)
    )


def _convert_record(
    value: Any,
    record_type: _types.RecordType,
    ctx: FlyteContext,
) -> _literals.Literal:
    """Convert a Python object to a Flyte record literal.

    Accepts dicts, dataclasses, or plain objects with attributes.
    Only fields declared in the record type are serialized.
    """
    if isinstance(value, dict):
        src = value
    elif is_dataclass(value):
        # Do NOT use dataclasses.asdict here as it recursively converts nested dataclasses
        # like LatchFile into dicts, breaking downstream blob conversions. Extract fields directly.
        src = {f.name: getattr(value, f.name) for f in dataclasses.fields(value)}
    elif hasattr(value, "__dict__"):
        src = {k: v for k, v in value.__dict__.items() if not k.startswith("_")}
    else:
        raise ValueError(f"Cannot convert {type(value)} to record")

    record_fields: list[_literals.RecordField] = []
    for field in record_type.fields:
        key = field.key
        sub_type = field.literal_type

        if key in src:
            sub_value = src[key]
        else:
            # Missing key: allow None if sub_type is Optional (Union with NONE)
            sub_value = None

        sub_literal = convert_python_value_to_literal(sub_value, sub_type, ctx)
        record_fields.append(_literals.RecordField(key=key, value=sub_literal))

    return _literals.Literal(record=_literals.Record(fields=record_fields))


def _convert_blob(
    value: Any,
    blob_type: _core_types.BlobType,
    ctx: FlyteContext,
) -> _literals.Literal:
    """Convert a file/directory path to a Flyte blob literal."""
    from latch.ldata.path import LPath
    from flytekit.core.type_engine import TypeEngine
    
    # Import file types
    try:
        from latch.types.file import LatchFile
        from latch.types.directory import LatchDir
    except ImportError:
        LatchFile = None
        LatchDir = None
    
    # Determine the appropriate type based on blob format
    if blob_type.format == "":
        # Generic blob - try to infer
        if isinstance(value, str):
            if LatchFile is not None:
                value = LatchFile(value)
        elif hasattr(value, "remote_path") or hasattr(value, "path"):
            # Already a file-like object
            pass
    
    # Use TypeEngine as fallback for blob types since they're well-defined
    try:
        if LatchFile is not None and isinstance(value, LatchFile):
            return TypeEngine.to_literal(ctx, value, LatchFile, None)
        elif LatchDir is not None and isinstance(value, LatchDir):
            return TypeEngine.to_literal(ctx, value, LatchDir, None)
        elif isinstance(value, LPath):
            return TypeEngine.to_literal(ctx, value, LPath, None)
        else:
            # Try converting string to LPath
            if isinstance(value, str):
                lpath = LPath(value)
                return TypeEngine.to_literal(ctx, lpath, LPath, None)
            raise ValueError(f"Cannot convert {type(value)} to blob")
    except Exception as e:
        raise ValueError(f"Failed to convert blob: {e}") from e


def _convert_enum(
    value: Any,
    enum_type: _core_types.EnumType,
) -> _literals.Literal:
    """Convert a Python Enum or string to an enum literal (string primitive)."""
    if isinstance(value, Enum):
        enum_val = value.value
    else:
        enum_val = value

    if not isinstance(enum_val, str):
        enum_val = str(enum_val)

    # Optional: enforce that the value is one of the declared enum values
    if enum_type.values and enum_val not in enum_type.values:
        raise ValueError(
            f"'{enum_val}' is not a valid value for enum {enum_type.values}"
        )

    prim = _literals.Primitive(string_value=enum_val)
    return _literals.Literal(scalar=_literals.Scalar(primitive=prim))


def _convert_union(
    value: Any,
    union_type: _types.LiteralType,
    ctx: FlyteContext,
) -> _literals.Literal:
    """
    Convert a value to a union type by trying each variant.
    
    For Optional types (Union with None), handle None specially.
    """
    if value is None:
        # Create a union literal with None
        none_literal = _create_none_literal()
        return _literals.Literal(
            scalar=_literals.Scalar(
                union=_literals.Union(
                    value=none_literal,
                    stored_type=_types.LiteralType(simple=_types.SimpleType.NONE),
                )
            )
        )
    
    # Try to convert to each variant
    variants = union_type.union_type.variants if union_type.union_type else []
    
    errors = []
    for variant in variants:
        try:
            converted = convert_python_value_to_literal(value, variant, ctx)
            return _literals.Literal(
                scalar=_literals.Scalar(
                    union=_literals.Union(
                        value=converted,
                        stored_type=variant,
                    )
                )
            )
        except Exception as e:
            errors.append((variant, str(e)))
            continue
    
    # If none of the variants worked, raise an error
    error_msg = f"Could not convert value to any union variant. Tried:\n"
    for variant, error in errors:
        error_msg += f"  - {variant}: {error}\n"
    raise ValueError(error_msg)


def convert_inputs_to_literals(
    ctx: FlyteContext,
    params: dict[str, Any],
    flyte_interface_types: dict[str, Any],  # Variable map
) -> dict[str, _literals.Literal]:
    """
    Convert a dictionary of Python values to Flyte Literals.
    
    This is the main entry point that replaces translate_inputs_to_literals
    with more flexible type conversion.
    
    Args:
        ctx: Flyte context
        params: Dictionary of parameter names to Python values
        flyte_interface_types: Dictionary of parameter names to Flyte Variables
        
    Returns:
        Dictionary of parameter names to Flyte Literals
    """
    result = {}
    
    for param_name, param_value in params.items():
        if param_name not in flyte_interface_types:
            raise ValueError(f"Parameter '{param_name}' not found in workflow interface")
        
        variable = flyte_interface_types[param_name]
        literal_type = variable.type
        
        try:
            result[param_name] = convert_python_value_to_literal(
                param_value,
                literal_type,
                ctx,
            )
        except Exception as e:
            raise ValueError(
                f"Failed to convert parameter '{param_name}' with value {param_value!r}: {e}"
            ) from e
    
    return result
