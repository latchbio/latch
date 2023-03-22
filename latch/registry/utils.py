import json
import keyword
import re
from dataclasses import fields, is_dataclass
from enum import Enum, EnumMeta
from typing import (
    Annotated,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

import latch.gql as gql
from latch.types import LatchDir, LatchFile

T = TypeVar("T")

account_root_regex = re.compile("^account_root\/(?P<root>[^/]+)(?P<path>\/.*)$")
mount_regex = re.compile("^mount\/(?P<mount>[^/]+)(?P<path>\/.*)$")

shared_regex = re.compile("^shared(?P<path>\/.*)$")


def clean(name: str):
    try:
        # https://stackoverflow.com/a/3305731
        cleaned = re.sub("\W|^(?=\d)", "_", name)

        if keyword.iskeyword(cleaned):
            cleaned = f"_{cleaned}"
        return cleaned
    except Exception as e:
        print(name)
        raise e


def to_python_type(
    registry_type: Dict,
    column_name: Optional[str] = None,
    allow_empty: bool = False,
):
    if column_name is not None:
        column_name = clean(column_name)

    ret = None
    if "primitive" in registry_type:
        primitive = registry_type["primitive"]
        if primitive in ["string", "datetime", "date"]:
            ret = str
        elif primitive == "integer":
            ret = int
        elif primitive == "number":
            ret = float
        elif primitive == "blob":
            if "metadata" in registry_type:
                if "nodeType" in registry_type["metadata"]:
                    if registry_type["metadata"]["nodeType"] == "dir":
                        ret = LatchDir
            ret = LatchFile
        elif primitive == "link":
            from latch.registry import Table

            id = registry_type["experimentId"]
            ret = Table(id).row_type
        elif primitive == "enum":
            ret = Enum(column_name or "Enum", registry_type["members"])
        elif primitive == "null":
            ret = type(None)
        elif primitive == "boolean":
            ret = bool
        else:
            raise ValueError(f"invalid primitive type: {registry_type['primitive']}")

    elif "array" in registry_type:
        ret = List[to_python_type(registry_type["array"], column_name, False)]

    elif "union" in registry_type:
        variants = []
        for key, variant in registry_type["union"].items():
            variants.append(
                Annotated[
                    to_python_type(
                        variant,
                        f"{column_name}_{key}",
                        False,
                    ),
                    key,
                ]
            )
        ret = Union[tuple(variants)]

    else:
        raise ValueError(
            "invalid registry type cannot be converted to a python type:"
            f" {registry_type}"
        )

    if allow_empty:
        return Optional[ret]
    return ret


def to_python_literal(
    registry_literal: Dict,
    registry_type: Dict,
):
    """converts a registry value to a python literal of provided
    python type, throws an exception on failure"""

    if not registry_literal["valid"]:
        raise ValueError(f"{registry_literal} is invalid, refusing to convert")

    value = registry_literal["value"]

    if "union" in registry_type:
        errors = {}

        for tag, sub_type in list(registry_type["union"].items()):
            if tag in value:
                try:
                    return to_python_literal(value[tag], sub_type)
                except Exception as e:
                    errors[str(sub_type)] = str(e)
                    pass

        raise ValueError(
            f"{value} cannot be converted into any union members of"
            f" {registry_type}:\n{json.dumps(errors, indent=2)}"
        )

    if "array" in registry_type:
        if not (type(value) is list):
            raise ValueError(
                f"{value} is not a list so it cannot be converted into a list"
            )

        return [to_python_literal(sub_val, registry_type["array"]) for sub_val in value]

    # python_type = to_python_type(registry_type)
    try:
        primitive = registry_type["primitive"]
    except KeyError:
        raise ValueError("cannot convert to python - malformed registry type")

    if primitive == "enum":
        try:
            members = map(clean, registry_type["members"])
        except KeyError:
            raise ValueError(
                "cannot convert to python - malformed registry enum type without"
                " members"
            )

        if clean(value) not in members:
            raise ValueError(
                f"unable to convert {value} ({clean(value)}) to any of the enum members"
                f" {members}"
            )

        return to_python_type(registry_type)._member_map_.get(clean(value))

    if primitive == "blob":
        typ = LatchFile
        if (
            "metadata" in registry_type
            and "nodeType" in registry_type["metadata"]
            and registry_type["metadata"]["nodeType"] == "dir"
        ):
            typ = LatchDir

        if not (type(value) is dict and ("ldataNodeId" in value)):
            raise ValueError("cannot convert non-blob value to blob type")

        path_data = gql.execute(
            """
            query nodePath($nodeId: BigInt!) {
                ldataOwner(argNodeId: $nodeId)
                ldataGetPath(argNodeId: $nodeId)
            }
            """,
            {"nodeId": value["ldataNodeId"]},
        )

        url = path_data["ldataGetPath"]
        owner = path_data["ldataOwner"]

        if url == None:
            raise ValueError("unable to convert to LatchFile/Dir as no url was found")

        account_match = account_root_regex.match(url)
        mount_match = mount_regex.match(url)

        path = ""

        if account_match is not None:
            if owner is None:
                raise ValueError(
                    "unable to convert to LatchFile/Dir as owner was not found"
                )

            path = f"latch://{owner}.account{account_match.groupdict()['path']}"
        elif mount_match is not None:
            groups = mount_match.groupdict()
            path = f"latch://{groups['mount']}.mount{groups['path']}"
        else:
            raise ValueError(
                f"unable to convert to LatchFile/Dir as url was malformed: {url}"
            )

        return typ(path)

    if primitive == "link":
        if "sampleId" not in value:
            raise ValueError("unable to convert to Link as row id was not found")
        try:
            table_id = registry_type["experimentId"]
        except KeyError:
            raise ValueError("Unable to convert link type without a table reference")

        from latch.registry.table import Table

        rows = Table(table_id).list()
        for row in rows:
            if row.id == value["sampleId"]:
                return row

        print(table_id, rows)
        raise ValueError(
            "unable to convert registry link to python - cannot find corresponding row"
            " in linked table"
        )

    python_type = to_python_type(registry_type)
    if isinstance(value, python_type):
        return value

    raise ValueError(
        f"primitive literal {registry_literal['value']} cannot be converted to"
        f" {python_type}"
    )


def to_registry_literal(
    python_literal,
    registry_type: Dict,
):
    if "union" in registry_type:
        errors = {}

        for tag, sub_type in registry_type["union"].items():
            try:
                return {
                    "tag": tag,
                    "value": to_registry_literal(python_literal, sub_type),
                }
            except Exception as e:
                errors[str(sub_type)] = str(e)
                pass

        raise ValueError(
            f"{python_literal} cannot be converted into any union members of"
            f" {json.dumps(registry_type, indent=2)}:\n{json.dumps(errors, indent=2)}"
        )

    if "array" in registry_type:
        sub_type = registry_type["array"]

        if not type(python_literal) is list:
            raise ValueError(
                "unable to convert non-list python literal to registry array literal"
            )

        return [to_registry_literal(literal, sub_type) for literal in python_literal]

    if not "primitive" in registry_type:
        raise ValueError(f"malformed registry type: {registry_type}")

    primitive = registry_type["primitive"]

    value = None
    if primitive in ["string", "date", "datetime"]:
        if not isinstance(python_literal, str):
            raise ValueError(
                f"cannot convert non-string python literal to {primitive} registry"
                " literal"
            )
        value = python_literal
    elif primitive == "integer":
        if not isinstance(python_literal, int):
            raise ValueError(
                "unable to convert non-int python literal to registry int literal"
            )
        value = int(python_literal)
    elif primitive == "number":
        if not (isinstance(python_literal, int) or isinstance(python_literal, float)):
            raise ValueError(
                "unable to convert non-numeric python literal to registry number"
                " literal"
            )
        value = float(python_literal)
    elif primitive == "boolean":
        if not isinstance(python_literal, bool):
            raise ValueError(
                "unable to convert non-boolean python literal to registry boolean"
                " literal"
            )
        value = python_literal
    elif primitive == "null":
        if not python_literal is None:
            raise ValueError(
                "unable to convert non-None python literal to registry null literal"
            )
    elif primitive == "enum":
        members = registry_type["members"]
        if isinstance(python_literal, Enum):
            python_literal = python_literal.name
        elif not isinstance(python_literal, str):
            python_literal = str(python_literal)
        if not python_literal in members:
            raise ValueError(
                f"unable to convert {python_literal} to registry enum with members"
                f" {', '.join(members)}"
            )
        value = python_literal
    elif primitive == "link":
        from latch.registry.row import Row

        if not isinstance(python_literal, Row):
            raise ValueError("cannot convert non-row python literal to registry link")
        value = {"sampleId": python_literal.id}
    elif primitive == "blob":
        if not (
            isinstance(python_literal, LatchFile)
            or isinstance(python_literal, LatchDir)
        ):
            raise ValueError("cannot convert non-blob python literal to registry blob")

        node_id = gql.execute(
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
        raise ValueError(f"malformed registry type {registry_type}")

    return {"value": value, "valid": True}
