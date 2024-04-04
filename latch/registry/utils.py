import json
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Optional, Type, TypeVar, Union, cast

import gql
from dateutil.parser import parse
from latch_sdk_config.user import user_config
from latch_sdk_gql.execute import execute

from latch.registry.record import Record
from latch.registry.types import InvalidValue, RegistryPythonValue
from latch.registry.upstream_types.types import (
    ArrayType,
    PrimitiveType,
    PrimitiveTypeEnum,
    RegistryType,
)
from latch.registry.upstream_types.values import DBValue
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.utils import current_workspace

# todo(maximsmol): hopefully, PyLance eventually narrows `TypedDict`` unions using `in`
# then we can get rid of the casts

T = TypeVar("T")


class RegistryTransformerException(ValueError): ...


def to_python_type(registry_type: RegistryType) -> Type[RegistryPythonValue]:
    if "primitive" in registry_type:
        primitive = cast(PrimitiveType, registry_type)["primitive"]
        if primitive == "string":
            return str
        if primitive == "datetime":
            return datetime
        if primitive == "date":
            return date
        if primitive == "integer":
            return int
        if primitive == "number":
            return float
        if primitive == "blob":
            return get_blob_nodetype(registry_type)
        if primitive == "link":
            return Record
        if primitive == "enum":
            members = cast(PrimitiveTypeEnum, registry_type)["members"]
            return Enum("Enum", members)
        if primitive == "null":
            return type(None)
        if primitive == "boolean":
            return bool

        raise RegistryTransformerException(f"invalid primitive type: {primitive}")

    if "array" in registry_type:
        array = cast(ArrayType, registry_type)["array"]
        return List[to_python_type(array)]

    if "union" in registry_type:
        variants: List[Type[RegistryPythonValue]] = []
        for variant in registry_type["union"].values():
            variants.append(
                # todo(maximsmol): allow specifying the exact variant we want
                # or preserving it when round-tripping?
                to_python_type(
                    variant,
                ),
            )
        return Union[tuple(variants)]

    raise RegistryTransformerException(
        f"unknown registry type cannot be converted to a python type: {registry_type}"
    )


def to_python_literal(
    registry_literal: DBValue,
    registry_type: RegistryType,
):
    if "array" in registry_type:
        if type(registry_literal) is not list:
            raise RegistryTransformerException(
                f"{registry_literal} is not a list so it cannot be converted into a"
                " list"
            )

        return [
            to_python_literal(sub_val, registry_type["array"])
            for sub_val in registry_literal
        ]
    if "union" in registry_type:
        tag = registry_literal["tag"]
        sub_type = registry_type["union"].get(tag)
        if sub_type is None:
            raise RegistryTransformerException(
                f"{registry_literal} cannot be converted to {registry_type} because its"
                f" tag `{tag}` is not present."
            )

        return to_python_literal(registry_literal["value"], sub_type)

    if not registry_literal["valid"]:
        return InvalidValue(registry_literal["rawValue"])

    value = registry_literal["value"]

    primitive = registry_type.get("primitive")
    if primitive is None:
        raise RegistryTransformerException(
            f"cannot convert to python - malformed registry type: {registry_type}"
        )

    if primitive == "enum":
        if "members" not in registry_type:
            raise RegistryTransformerException(
                "cannot convert to python - malformed registry enum type without"
                f" members: {registry_type}"
            )

        members = registry_type["members"]
        if value not in members:
            raise RegistryTransformerException(
                f"unable to convert {value} to any of the enum members {members}"
            )

        return to_python_type(registry_type)[value]

    if primitive == "blob":
        typ = get_blob_nodetype(registry_type)

        if type(value) is not dict or "ldataNodeId" not in value:
            raise RegistryTransformerException(
                f"cannot convert non-blob value {value} to blob type"
            )

        return typ(f"latch://{value['ldataNodeId']}.node")

    if primitive == "link":
        if "sampleId" not in value:
            raise RegistryTransformerException(
                f"unable to convert {value} to Link as row id was not found"
            )

        from latch.registry.record import Record

        return Record(value["sampleId"])

    if primitive == "string":
        if isinstance(value, str):
            return value
        raise RegistryTransformerException(
            f"Cannot convert {value} to string as it is not a primitive string literal"
        )

    if primitive == "number":
        if isinstance(value, float) or isinstance(value, int):
            return float(value)

        raise RegistryTransformerException(
            f"Cannot convert {value} ({type(value)}) to float as it is not a primitive"
            " number literal"
        )

    if primitive == "integer":
        if isinstance(value, int):
            return value
        raise RegistryTransformerException(
            f"Cannot convert {value} to integer as it is not a primitive integer"
            " literal"
        )

    if primitive == "boolean":
        if isinstance(value, bool):
            return value
        raise RegistryTransformerException(
            f"Cannot convert {value} to boolean as it is not a primitive boolean"
            " literal"
        )

    if primitive == "date":
        if isinstance(value, str):
            return parse(value).date()
        raise RegistryTransformerException(
            f"Cannot convert {value} to date as it is not a primitive date literal"
        )

    if primitive == "datetime":
        if isinstance(value, str):
            return parse(value)
        raise RegistryTransformerException(
            f"Cannot convert {value} to datetime as it is not a primitive datetime"
            " literal"
        )

    if primitive == "null":
        if value is None:
            return value
        raise RegistryTransformerException(
            f"Cannot convert {value} to None as it is not a primitive null literal"
        )

    raise ValueError(
        f"primitive literal {registry_literal['value']} cannot be converted to"
        f" {registry_type}"
    )


def to_registry_literal(
    python_literal,
    registry_type: RegistryType,
) -> DBValue:
    if isinstance(python_literal, InvalidValue):
        return {"valid": False, "rawValue": python_literal.raw_value}

    if "union" in registry_type:
        errors: Dict[str, str] = {}

        for tag, sub_type in registry_type["union"].items():
            try:
                return {
                    "tag": tag,
                    "value": to_registry_literal(python_literal, sub_type),
                }
            except RegistryTransformerException as e:
                errors[str(sub_type)] = str(e)
                pass

        raise RegistryTransformerException(
            f"{python_literal} cannot be converted into any union members of"
            f" {json.dumps(registry_type, indent=2)}:\n{json.dumps(errors, indent=2)}"
        )

    if "array" in registry_type:
        sub_type = registry_type["array"]

        if type(python_literal) is not list:
            raise RegistryTransformerException(
                "unable to convert non-list python literal to registry array literal"
            )

        return [to_registry_literal(literal, sub_type) for literal in python_literal]

    if "primitive" not in registry_type:
        raise RegistryTransformerException(f"malformed registry type: {registry_type}")

    primitive = registry_type["primitive"]

    value: Optional[object] = None
    if primitive == "string":
        if not isinstance(python_literal, str):
            raise RegistryTransformerException(
                f"cannot convert non-string python literal to {primitive} registry"
                " literal"
            )

        value = python_literal
    elif primitive in ["date", "datetime"]:
        # datetime is a subclass of date
        if not isinstance(python_literal, date):
            raise RegistryTransformerException(
                f"cannot convert non-date(time) python literal to {primitive} registry"
                " literal"
            )

        value = python_literal.isoformat()
    elif primitive == "integer":
        if not isinstance(python_literal, int):
            raise RegistryTransformerException(
                "unable to convert non-int python literal to registry int literal"
            )

        value = int(python_literal)
    elif primitive == "number":
        if not (isinstance(python_literal, int) or isinstance(python_literal, float)):
            raise RegistryTransformerException(
                "unable to convert non-numeric python literal to registry number"
                " literal"
            )

        value = float(python_literal)
    elif primitive == "boolean":
        if not isinstance(python_literal, bool):
            raise RegistryTransformerException(
                "unable to convert non-boolean python literal to registry boolean"
                " literal"
            )

        value = python_literal
    elif primitive == "null":
        if not python_literal is None:
            raise RegistryTransformerException(
                "unable to convert non-None python literal to registry null literal"
            )

    elif primitive == "enum":
        members = registry_type["members"]

        if isinstance(python_literal, Enum):
            python_literal = python_literal.name
        elif not isinstance(python_literal, str):
            python_literal = str(python_literal)
        if not python_literal in members:
            raise RegistryTransformerException(
                f"unable to convert {python_literal} to registry enum with members"
                f" {', '.join(members)}"
            )

        value = python_literal
    elif primitive == "link":
        if not isinstance(python_literal, Record):
            raise RegistryTransformerException(
                "cannot convert non-record python literal to registry link"
            )

        value = {"sampleId": python_literal.id}
    elif primitive == "blob":
        if not (
            isinstance(python_literal, LatchFile)
            or isinstance(python_literal, LatchDir)
        ):
            raise RegistryTransformerException(
                "cannot convert non-blob python literal to registry blob"
            )

        ws_id = current_workspace()
        if ws_id == "":
            ws_id = None

        if ws_id is None:
            data = execute(
                gql.gql("""
                query nodeIdQ($argPath: String!) {
                    ldataResolvePath(
                        path: $argPath
                    ) {
                        nodeId
                        path
                    }
                }
                """),
                {"argPath": python_literal.remote_path},
            )["ldataResolvePath"]
        else:
            data = execute(
                gql.gql("""
                query nodeIdQ($argPath: String!, $wsId: BigInt!) {
                    ldataResolvePathExt(
                        path: $argPath,
                        accId: $wsId
                    ) {
                        nodeId
                        path
                    }
                }
                """),
                {"argPath": python_literal.remote_path, "wsId": ws_id},
            )["ldataResolvePathExt"]

        if data["path"] is not None and data["path"] != "":
            # todo(maximsmol): store an invalid value instead?
            raise RegistryTransformerException(
                f"could not resolve path: {python_literal.remote_path}"
            )

        value = {"ldataNodeId": data["nodeId"]}
    else:
        raise RegistryTransformerException(f"malformed registry type: {registry_type}")

    return {"value": value, "valid": True}


def get_blob_nodetype(
    registry_type: RegistryType,
) -> Union[Type[LatchFile], Type[LatchDir]]:
    if "primitive" not in registry_type or registry_type["primitive"] != "blob":
        raise RegistryTransformerException(
            f"cannot extract blob nodetype from non-blob type"
        )

    if (
        "metadata" in registry_type
        and "nodeType" in registry_type["metadata"]
        and registry_type["metadata"]["nodeType"] == "dir"
    ):
        return LatchDir

    return LatchFile
