try:
    from typing import get_args, get_origin
except ImportError:
    from typing_extensions import get_args, get_origin

import dataclasses
import enum
import json
import keyword
import typing
from typing import Optional

import google.protobuf.json_format as gpjson
from dataclasses_json import dataclass_json
from flyteidl.core.literals_pb2 import Literal as _Literal
from flyteidl.core.types_pb2 import LiteralType as _LiteralType
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType

from latch.types import LatchDir, LatchFile
from latch_cli.services.launch import _get_workflow_interface
from latch_cli.utils import retrieve_or_login


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
    9: _Unsupported,
}

_primitive_table = {
    type(None): None,
    int: 0,
    float: 0.0,
    str: "foo",
    bool: False,
}

# TODO(ayush): fix this to
# (1) support records,
# (2) support fully qualified workflow names,
# (note from kenny) - pretty sure you intend to support the opposite,
# fqn are supported by default, address when you get to this todo
# (3) show a message indicating the generated filename,
# (4) optionally specify the output filename


def get_params(wf_name: str, wf_version: Optional[str] = None):
    """Constructs a parameter map for a workflow given its name and an optional
    version.

    This function creates a python parameter file that can be used by `launch`.
    You can specify the specific parameters by editing the file, and then launch
    an execution on Latch using those parameters with `launch`.

    Args:
        wf_name: The unique name of the workflow.
        wf_version: An optional workflow version. If this argument is not given,
            `get_params` will default to generating a parameter map of the most
            recent version of the workflow.

    Example:
        >>> get_params("wf.__init__.alphafold_wf")
            # creates a file called `wf.__init__.alphafold_wf.params.py` that
            # contains a template parameter map.
    """

    token = retrieve_or_login()
    wf_id, wf_interface, wf_default_params = _get_workflow_interface(
        token, wf_name, wf_version
    )

    params = {}
    wf_vars = wf_interface["variables"]
    default_wf_vars = wf_default_params["parameters"]
    for key, value in wf_vars.items():
        try:
            description_json_str = value["description"]
            literal_type_json = value["type"]
        except KeyError as e:
            raise ValueError(
                f"Flyte workflow interface for "
                f"{wf_name}-{wf_version} is missing 'description' or "
                "'type' key"
            ) from e

        try:
            description_json = json.loads(description_json_str)
        except json.decoder.JSONDecodeError:
            # Parameters that are used for control flow in forks do not have
            # valid JSON in description and we can safely ignore these.
            # TODO - add metadata to control flow fork parameters to exclude
            # these specifically.
            continue

        try:
            param_name = description_json["name"]
        except KeyError as e:
            raise ValueError(
                f"Parameter description json for workflow {wf_name} and parameter {key} is missing 'name' key."
            ) from e

        literal_type = gpjson.ParseDict(literal_type_json, _LiteralType())

        python_type = _guess_python_type(
            LiteralType.from_flyte_idl(literal_type), param_name
        )

        default = True
        if default_wf_vars[param_name].get("required") is not True:
            literal_json = default_wf_vars[param_name].get("default")
            literal = gpjson.ParseDict(literal_json, _Literal())
            val = _guess_python_val(Literal.from_flyte_idl(literal), python_type)
        else:
            default = False

            val = _best_effort_default_val(python_type)

        params[param_name] = (python_type, val, default)

    import_statements = {
        LatchFile: "from latch.types import LatchFile",
        LatchDir: "from latch.types import LatchDir",
        enum.Enum: "from enum import Enum",
    }

    import_types = []
    enum_literals = []
    param_map_str = ""
    param_map_str += "\nparams = {"
    param_map_str += f'\n    "_name": "{wf_name}", # Don\'t edit this value.'
    for param_name, value in params.items():
        python_type, python_val, default = value

        # Check for imports.

        def _check_and_import(python_type: typing.T):
            if python_type in import_statements and python_type not in import_types:
                import_types.append(python_type)

        def _handle_enum(python_type: typing.T):
            if type(python_type) is enum.EnumMeta:
                if enum.Enum not in import_types:
                    import_types.append(enum.Enum)

                variants = python_type._variants
                name = python_type._name

                _enum_literal = f"class {name}(Enum):"
                for variant in variants:
                    if variant in keyword.kwlist:
                        variant_name = f"_{variant}"
                    else:
                        variant_name = variant
                    _enum_literal += f"\n    {variant_name} = '{variant}'"
                enum_literals.append(_enum_literal)

        # Parse collection, union types for potential imports and dependent
        # objects, eg. enum class construction.
        if get_origin(python_type) is not None:
            if get_origin(python_type) is list:
                _check_and_import(get_args(python_type)[0])
                _handle_enum(get_args(python_type)[0])
            elif get_origin(python_type) is typing.Union:
                for variant in get_args(python_type):
                    _check_and_import(variant)
                    _handle_enum(variant)
        else:
            _check_and_import(python_type)
            _handle_enum(python_type)

        python_val, python_type = _get_code_literal(python_val, python_type)

        if default is True:
            default = "DEFAULT. "
        else:
            default = ""

        param_map_str += f'\n    "{param_name}": {python_val}, # {default}{python_type}'
    param_map_str += "\n}"

    with open(f"{wf_name}.params.py", "w") as f:

        f.write(
            f'"""Run `latch launch {wf_name}.params.py` to launch this workflow"""\n'
        )

        for t in import_types:
            f.write(f"\n{import_statements[t]}")
        for e in enum_literals:
            f.write(f"\n\n{e}\n")

        f.write("\n")
        f.write(param_map_str)


def _get_code_literal(python_val: any, python_type: typing.T):
    """Construct value that is executable python when templated into a code
    block."""

    if python_type is str or (type(python_val) is str and str in get_args(python_type)):
        return f'"{python_val}"', python_type

    if type(python_type) is enum.EnumMeta:
        name = python_type._name
        return python_val, f"<enum '{name}'>"

    if get_origin(python_type) is typing.Union:
        variants = get_args(python_type)
        type_repr = "typing.Union["
        for i, variant in enumerate(variants):
            if i < len(variants) - 1:
                delimiter = ", "
            else:
                delimiter = ""
            type_repr += f"{_get_code_literal(python_val, variant)[1]}{delimiter}"
        type_repr += "]"
        return python_val, type_repr

    if get_origin(python_type) is list:
        if python_val is None:
            _, type_repr = _get_code_literal(None, get_args(python_type)[0])
            return None, f"typing.List[{type_repr}]"
        else:
            collection_literal = "["
            if len(python_val) > 0:
                for i, item in enumerate(python_val):
                    item_literal, type_repr = _get_code_literal(
                        item, get_args(python_type)[0]
                    )

                    if i < len(python_val) - 1:
                        delimiter = ","
                    else:
                        delimiter = ""

                    collection_literal += f"{item_literal}{delimiter}"
            else:
                list_t = get_args(python_type)[0]
                _, type_repr = _get_code_literal(
                    _best_effort_default_val(list_t), list_t
                )

            collection_literal += "]"
            return collection_literal, f"typing.List[{type_repr}]"

    return python_val, python_type


def _guess_python_val(literal: _Literal, python_type: typing.T):
    """Transform flyte literal value to native python value."""

    if literal.scalar is not None:
        if literal.scalar.none_type is not None:
            return None

        if literal.scalar.primitive is not None:
            primitive = literal.scalar.primitive

            if primitive.string_value is not None:
                if type(python_type) is enum.EnumMeta:
                    return f"{python_type._name}.{str(primitive.string_value)}"
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
            p_list.append(_guess_python_val(item, get_args(python_type)[0]))
        return p_list

    # sum

    # enum

    raise NotImplementedError(
        f"The flyte literal {literal} cannot be transformed to a python type."
    )


def _guess_python_type(literal_type: LiteralType, param_name: str):
    """Transform Flyte LiteralType to Python type."""

    if literal_type.simple is not None:
        if literal_type.simple == 9:

            from flytekit.core.type_engine import DataclassTransformer

            def get_schema_name(metadata: dict) -> str:
                return metadata["$ref"].split("/")[-1]

            def _guess_property_python_type(
                property_val: dict, json_schema: dict
            ) -> str:

                if "oneOf" in property_val:

                    for x in property_val["oneOf"]:
                        print("thing: ")
                        print(x)
                        print()
                    print()

                    return typing.Union[
                        tuple(
                            [
                                _guess_property_python_type(
                                    variant["properties"]["type"]["const"], json_schema
                                )
                                for variant in property_val["oneOf"]
                            ]
                        )
                    ]

                print(property_val)
                property_type = property_val["type"]
                if property_type == "object":
                    if property_val.get("$ref"):
                        nested_name = get_schema_name(property_val)
                        return construct_class_from_json_schema(
                            json_schema, nested_name
                        )
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
                    element_format = (
                        property_val["format"] if "format" in property_val else None
                    )
                    if element_format == "integer":
                        return int
                    else:
                        return float
                print("property_val: ", property_val)
                print("property_schema: ", json_schema)
                print()
                return str

            def construct_class_from_json_schema(json_schema: dict, schema_name: str):
                attribute_list = []
                for property_key, property_val in json_schema[schema_name][
                    "properties"
                ].items():
                    attribute_list.append(
                        (
                            property_key,
                            _guess_property_python_type(property_val, json_schema),
                        )
                    )

                for x in attribute_list:
                    print(x)

                return dataclass_json(
                    dataclasses.make_dataclass(schema_name, attribute_list)
                )

            def guess_python_class(literal_type: LiteralType):

                if (
                    literal_type.metadata is not None
                    and "definitions" in literal_type.metadata
                ):
                    schema_name = get_schema_name(literal_type.metadata)
                    return construct_class_from_json_schema(
                        literal_type.metadata["definitions"], schema_name
                    )

                raise ValueError(
                    f"Unable to guess dataclass for literal_type: {literal_type}"
                )

            return guess_python_class(literal_type)
        return _simple_table[literal_type.simple]

    if literal_type.collection_type is not None:
        return typing.List[_guess_python_type(literal_type.collection_type, param_name)]

    if literal_type.blob is not None:

        # flyteidl BlobType message for reference:
        #   enum BlobDimensionality {
        #       SINGLE = 0;
        #       MULTIPART = 1;
        #   }

        dim = literal_type.blob.dimensionality
        if dim == 0:
            return LatchFile
        else:
            return LatchDir

    if literal_type.union_type is not None:

        variant_types = [
            _guess_python_type(variant, param_name)
            for variant in literal_type.union_type.variants
        ]

        # Trying to directly construct set of types will throw error if list is
        # included as 'list' is not hashable.
        unique_variants = []
        for t in variant_types:
            if t not in unique_variants:
                unique_variants.append(t)

        return typing.Union[tuple(variant_types)]

    if literal_type.enum_type is not None:

        # we can parse the variants and define the object in the param map
        # code.

        class _VariantCarrier(enum.Enum):
            ...

        _VariantCarrier._variants = literal_type.enum_type.values
        # Use param name to uniquely identify each enum
        _VariantCarrier._name = param_name
        return _VariantCarrier

    raise NotImplementedError(
        f"The flyte literal_type {literal_type} cannot be transformed to a python type."
    )


def _best_effort_default_val(t: typing.T):
    """Produce a "best-effort" default value given a python type."""

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
        return f"{t._name}.{t._variants[0]}"

    if get_origin(t) is None:
        raise NotImplementedError(
            f"Unable to produce a best-effort value for the python type {t}"
        )

    if get_origin(t) is list:
        list_args = get_args(t)
        if len(list_args) == 0:
            return []
        return [_best_effort_default_val(arg) for arg in list_args]

    if get_origin(t) is typing.Union:
        return _best_effort_default_val(get_args(t)[0])

    raise NotImplementedError(
        f"Unable to produce a best-effort value for the python type {t}"
    )
