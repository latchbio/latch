import json
from dataclasses import dataclass
from pathlib import Path
from textwrap import indent
from typing import Generic, Literal, Optional, TypedDict, TypeVar, Union

import click
from typing_extensions import NotRequired, TypeAlias

# todo(ayush): fix typing for defaults
# todo(ayush): lots of type errors here, don't think any are that serious but would like to fix eventually


class SchemaParsingError(RuntimeError):
    def __init__(self, msg: str, children: Optional[list[Exception]] = None):
        self.msg = msg
        self.children = children

    def __str__(self):
        if self.children is None:
            return self.msg

        res: list[str] = [self.msg]
        res.extend([indent(str(child), "  ") for child in self.children])

        return "\n".join(res)


class CommonMetadata(TypedDict):
    title: NotRequired[str]
    description: NotRequired[str]
    help_text: NotRequired[str]
    errorMessage: NotRequired[str]
    required: bool


class NfStringType(TypedDict):
    type: Literal["string"]
    default: NotRequired[str]
    metadata: NotRequired[CommonMetadata]
    regex: NotRequired[str]


class NfIntegerType(TypedDict):
    type: Literal["integer"]
    default: NotRequired[int]
    min: NotRequired[int]
    max: NotRequired[int]
    metadata: NotRequired[CommonMetadata]


class NfFloatType(TypedDict):
    type: Literal["number"]
    default: NotRequired[float]
    min: NotRequired[float]
    max: NotRequired[float]
    metadata: NotRequired[CommonMetadata]


class NfBooleanType(TypedDict):
    type: Literal["boolean"]
    default: NotRequired[bool]
    metadata: NotRequired[CommonMetadata]


EnumT = TypeVar("EnumT", str, int, float)


class NfEnumType(TypedDict, Generic[EnumT]):
    type: Literal["enum"]
    flavor: Literal["string", "integer", "number"]
    values: list[EnumT]
    default: NotRequired[EnumT]
    metadata: NotRequired[CommonMetadata]


class NfArrayType(TypedDict):
    type: Literal["array"]
    default: NotRequired[list]
    items: "NfType"
    metadata: NotRequired[CommonMetadata]


class NfObjectType(TypedDict):
    type: Literal["object"]
    default: NotRequired[dict]
    properties: dict[str, "NfType"]
    metadata: NotRequired[CommonMetadata]


class NfSamplesheetType(TypedDict):
    type: Literal["samplesheet"]
    default: NotRequired[list[dict]]
    metadata: NotRequired[CommonMetadata]
    schema: dict[str, "NfType"]


class NfBlobType(TypedDict):
    type: Literal["blob"]
    node_type: Literal["file", "dir", "any"]
    metadata: NotRequired[CommonMetadata]
    regex: NotRequired[str]


NfType: TypeAlias = Union[
    NfStringType,
    NfIntegerType,
    NfFloatType,
    NfEnumType[str],
    NfEnumType[float],
    NfEnumType[int],
    NfBooleanType,
    NfArrayType,
    NfObjectType,
    NfBlobType,
    NfSamplesheetType,
]


def get_common_metadata(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> CommonMetadata:
    ret: CommonMetadata = {"required": param_name in required_set}

    for x in ["title", "description", "help_text", "errorMessage"]:
        if x not in properties:
            continue

        ret[x] = properties[x]

    return ret


def parse_string(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> Union[NfStringType, NfBlobType, NfSamplesheetType]:
    assert properties["type"] == "string"

    metadata = get_common_metadata(param_name, properties, required_set)

    format = properties.get("format")
    regex = properties.get("pattern")
    default = properties.get("default")

    assert format is None or isinstance(format, str)
    assert regex is None or isinstance(regex, str)

    # todo(ayush): treating glob as a string for now
    if format is None or format == "file-path-pattern":
        return NfStringType(
            type="string",
            metadata=metadata,
            **({"regex": regex} if regex is not None else {}),
            **({"default": default} if default is not None else {}),
        )

    assert format in {"uri", "file-path", "directory-path", "path"}

    if "schema" in properties and format == "file-path":
        schema_ref = properties["schema"]
        assert isinstance(schema_ref, str)
        assert default is None or isinstance(default, list)

        parse_res = parse_samplesheet_schema(Path(schema_ref))
        default = parse_res.default

        return NfSamplesheetType(
            type="samplesheet",
            metadata=metadata,
            schema=parse_res.schema,
            **({} if default is None else {"default": default}),
        )

    assert default is None or isinstance(default, str)

    node_type: Literal["file", "dir", "any"] = "any"
    if format == "file-path":
        node_type = "file"
    if format == "directory-path":
        node_type = "dir"

    return NfBlobType(
        type="blob",
        node_type=node_type,
        metadata=metadata,
        **({"regex": regex} if regex is not None else {}),
        **({"default": default} if default is not None else {}),
    )


def parse_integer(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> NfIntegerType:
    assert properties["type"] == "integer"

    metadata = get_common_metadata(param_name, properties, required_set)

    min = properties.get("minimum")
    max = properties.get("maximum")
    default = properties.get("default")

    assert min is None or isinstance(min, int)
    assert max is None or isinstance(max, int)
    assert default is None or isinstance(default, int)

    return NfIntegerType(
        type="integer",
        metadata=metadata,
        **({"min": min} if min is not None else {}),
        **({"max": max} if max is not None else {}),
        **({"default": default} if default is not None else {}),
    )


def parse_float(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> NfFloatType:
    assert properties["type"] == "number"

    metadata = get_common_metadata(param_name, properties, required_set)

    min = properties.get("minimum")
    max = properties.get("maximum")
    default = properties.get("default")

    assert min is None or isinstance(min, (int, float))
    assert max is None or isinstance(max, (int, float))
    assert default is None or isinstance(default, (int, float))

    return NfFloatType(
        type="number",
        metadata=metadata,
        **({"min": min} if min is not None else {}),
        **({"max": max} if max is not None else {}),
        **({"default": default} if default is not None else {}),
    )


def parse_bool(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> NfBooleanType:
    assert properties["type"] == "boolean"

    metadata = get_common_metadata(param_name, properties, required_set)
    default = properties.get("default")

    assert default is None or isinstance(default, bool)

    return NfBooleanType(
        type="boolean",
        metadata=metadata,
        **({"default": default} if default is not None else {}),
    )


def parse_enum(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> Union[NfEnumType[str], NfEnumType[int], NfEnumType[float]]:
    typ = properties["type"]

    assert typ in {"string", "number", "integer"}
    assert "enum" in properties

    metadata = get_common_metadata(param_name, properties, required_set)

    default = properties.get("default")
    values = properties.get("enum")

    assert values is not None

    # todo(ayush): fix type errors here
    return NfEnumType(
        type="enum",
        flavor=typ,
        values=values,
        metadata=metadata,
        **({"default": default} if default is not None else {}),
    )


def parse_array(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> NfArrayType:
    assert properties["type"] == "array"

    metadata = get_common_metadata(param_name, properties, required_set)
    default = properties.get("default")
    items = properties.get("items")

    if items is None:
        raise ValueError("malformed array type: no 'item' type specified")

    # todo(ayush): this is kind of weird tbh, doesn't make sense for certain metadata like
    # "required" to apply here
    items = parse("", items, set())

    # todo(ayush): fix type errors here too
    return NfArrayType(
        type="array",
        items=items,
        metadata=metadata,
        **({"default": default} if default is not None else {}),
    )


def parse_object(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> NfObjectType:
    assert properties["type"] == "object"

    metadata = get_common_metadata(param_name, properties, required_set)

    default = properties.get("default")
    required = properties.get("required")
    fields = properties.get("properties")

    if fields is None:
        raise ValueError("malformed object type: no 'properties' dict specified")

    required_fields = set(required if required is not None else [])

    res: dict[str, NfType] = {}
    for field_name, field_properties in fields.items():
        res[field_name] = parse(field_name, field_properties, required_fields)

    return NfObjectType(
        type="object",
        properties=res,
        metadata=metadata,
        **({"default": default} if default is not None else {}),
    )


def parse(
    param_name: str, properties: dict[str, object], required_set: set[str]
) -> NfType:
    if "type" not in properties:
        raise SchemaParsingError("malformed type spec: no `type` key provided")

    typ = properties["type"]

    try:
        if "enum" in properties:
            return parse_enum(param_name, properties, required_set)
        if typ == "string":
            return parse_string(param_name, properties, required_set)
        if typ == "integer":
            return parse_integer(param_name, properties, required_set)
        if typ == "number":
            return parse_float(param_name, properties, required_set)
        if typ == "boolean":
            return parse_bool(param_name, properties, required_set)
        if typ == "array":
            return parse_array(param_name, properties, required_set)
        if typ == "object":
            return parse_object(param_name, properties, required_set)
    except Exception as e:
        if "enum" in properties:
            typ = "enum"

        raise SchemaParsingError(
            f"error parsing {param_name!r}: invalid {typ!r} type entry {properties!r}",
            [e],
        ) from e

    raise ValueError(f"unknown type: {typ!r}")


@dataclass
class ParseSamplesheetRes:
    schema: dict[str, NfType]
    default: Optional[list[dict]] = None


def parse_samplesheet_schema(schema_path: Path) -> ParseSamplesheetRes:
    try:
        schema_contents = json.loads(schema_path.read_text())
    except OSError as e:
        click.secho(
            f"Could not find samplesheet schema file at {schema_path}.", fg="red"
        )
        raise click.exceptions.Exit(1) from e

    # note(ayush): samplesheet schema files are an array with item type object. The object's
    # schema is the samplesheet schema
    array = parse_array("", schema_contents, set())

    return ParseSamplesheetRes(array["items"]["properties"], array.get("default"))


def parse_schema(schema_path: Path) -> dict[str, NfType]:
    try:
        schema_contents = json.loads(schema_path.read_text())
    except OSError as e:
        click.secho(f"Could not find schema file at {schema_path}.", fg="red")
        raise click.exceptions.Exit(1) from e

    if "allOf" not in schema_contents:
        return {}

    schema_components = schema_contents["allOf"]

    params = {}

    for x in schema_components:
        if "$ref" not in x:
            raise ValueError("`$ref` not found in `allOf` entry")

        ref = x["$ref"]
        assert isinstance(ref, str)

        if not ref.startswith("#"):
            params = {**params, **parse_schema(Path(ref))}
            continue

        try:
            src = schema_contents
            for key in ref.strip("#").split("/"):
                if key == "":
                    continue

                src = src[key]
        except KeyError as e:
            raise ValueError(f"invalid $ref: {ref!r}") from e

        if "properties" not in src:
            raise ValueError(
                f"invalid parameter grouping: key 'properties' not found in ref {ref!r}"
            )

        properties = src["properties"]
        assert isinstance(properties, dict)

        required = set(src.get("required", []))

        for param_name, props in properties.items():
            try:
                params[param_name] = parse(param_name, props, required)
            except SchemaParsingError as e:
                click.secho(f"Unable to parse {schema_path}\n{e}", fg="red")
                raise click.exceptions.Exit(1) from e

    return params
