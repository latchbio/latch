import json
import re
from datetime import date, datetime
from enum import Enum
from typing import Annotated, Dict, List, Optional, Type, TypeVar, Union

from dateutil.parser import parse

from latch.gql.execute import execute
from latch.registry.types import (
    EmptyCell,
    InvalidValue,
    RegistryDBValue,
    RegistryPrimitiveBlobValue,
    RegistryPrimitiveSimpleType,
    RegistryPrimitiveType,
    RegistryType,
)
from latch.types import LatchDir, LatchFile

T = TypeVar("T")


account_root_regex = re.compile("^account_root\/(?P<root>[^/]+)(?P<path>\/.*)$")
mount_regex = re.compile("^mount\/(?P<mount>[^/]+)(?P<path>\/.*)$")
shared_regex = re.compile("^shared(?P<path>\/.*)$")


class RegistryTransformerException(Exception):
    ...


def to_python_type(
    registry_type: RegistryType,
    *_ignored_args,
    allow_empty: bool = False,
) -> Type:
    ret: Optional[Type] = None
    if "primitive" in registry_type:
        primitive = registry_type["primitive"]
        if primitive == "string":
            ret = str
        elif primitive == "datetime":
            ret = datetime
        elif primitive == "date":
            ret = date
        elif primitive == "integer":
            ret = int
        elif primitive == "number":
            ret = float
        elif primitive == "blob":
            ret = get_blob_nodetype(registry_type)
        elif primitive == "link":
            from latch.registry.record import Record

            ret = Record
        elif primitive == "enum":
            ret = Enum("Enum", registry_type["members"])
        elif primitive == "null":
            ret = type(None)
        elif primitive == "boolean":
            ret = bool
        else:
            raise RegistryTransformerException(
                f"invalid primitive type: {registry_type['primitive']}"
            )

    elif "array" in registry_type:
        ret = List[to_python_type(registry_type["array"], allow_empty=False)]

    elif "union" in registry_type:
        variants: List[Type] = []
        for key, variant in registry_type["union"].items():
            variants.append(
                Annotated[
                    to_python_type(
                        variant,
                        allow_empty=False,
                    ),
                    key,
                ]
            )
        ret = Union[tuple(variants)]

    else:
        raise RegistryTransformerException(
            "unknown registry type cannot be converted to a python type:"
            f" {registry_type}"
        )

    if allow_empty:
        return Union[ret, EmptyCell]
    return ret


def to_python_literal(
    registry_literal: RegistryDBValue,
    registry_type: RegistryType,
):
    """converts a registry value to a python literal of provided
    python type, throws an exception on failure"""

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

    if not registry_literal["valid"]:
        return InvalidValue(registry_literal["rawValue"])

    value = registry_literal["value"]

    if "union" in registry_type:
        tag = registry_literal["tag"]
        sub_type = registry_type["union"].get(tag)
        if sub_type is None:
            raise RegistryTransformerException(
                f"{value} cannot be converted to {registry_type} because its tag"
                f" `{tag}` is not present."
            )

        return to_python_literal(value["value"], sub_type)

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

        path_data = execute(
            """
            query nodePath($nodeId: BigInt!) {
                ldataOwnerUnsafe(argNodeId: $nodeId)
                ldataGetPath(argNodeId: $nodeId)
            }
            """,
            {"nodeId": value["ldataNodeId"]},
        )

        url = path_data["ldataGetPath"]
        owner = path_data["ldataOwnerUnsafe"]

        if url == None:
            raise RegistryTransformerException(
                f"unable to convert blob with id {value['ldataNodeId']} to"
                " LatchFile/Dir as no url was found"
            )

        account_match = account_root_regex.match(url)
        mount_match = mount_regex.match(url)

        path = ""

        if account_match is not None:
            if owner is None:
                raise RegistryTransformerException(
                    f"unable to convert blob with id {value['ldataNodeId']} to"
                    " LatchFile/Dir as owner was not found"
                )

            path = f"latch://{owner}.account{account_match.groupdict()['path']}"
        elif mount_match is not None:
            groups = mount_match.groupdict()
            path = f"latch://{groups['mount']}.mount{groups['path']}"
        else:
            raise RegistryTransformerException(
                f"unable to convert to LatchFile/Dir as url was malformed: {url}"
            )

        return typ(path)

    if primitive == "link":
        if "sampleId" not in value:
            raise RegistryTransformerException(
                f"unable to convert {value} to Link as row id was not found"
            )

        from latch.registry.record import Record

        return Record.from_id(value["sampleId"])

    if primitive == "string":
        if isinstance(value, str):
            return value
        raise RegistryTransformerException(
            f"Cannot convert {value} to string as it is not a primitive string literal"
        )

    if primitive == "number":
        if isinstance(value, float):
            return value
        raise RegistryTransformerException(
            f"Cannot convert {value} to float as it is not a primitive number literal"
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
) -> RegistryDBValue:
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
        from latch.registry.record import Record

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

        node_id = execute(
            """
            query nodeIdQ($argPath: String!) {
                ldataResolvePath(
                    path: $argPath
                ) {
                    nodeId
                }
            }
            """,
            {"argPath": python_literal.remote_path},
        )["ldataResolvePath"]["nodeId"]

        value = {"ldataNodeId": node_id}
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
