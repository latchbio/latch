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

from latch.type_engine import (
    best_effort_python_val,
    guess_python_type,
    guess_python_val,
)
from latch.types import LatchDir, LatchFile
from latch_cli.services.launch import _get_workflow_interface
from latch_cli.utils import retrieve_or_login

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

        python_type = guess_python_type(LiteralType.from_flyte_idl(literal_type))

        default = True
        if default_wf_vars[param_name].get("required") is not True:
            literal_json = default_wf_vars[param_name].get("default")
            literal = gpjson.ParseDict(literal_json, _Literal())
            val = guess_python_val(Literal.from_flyte_idl(literal), python_type)
        else:
            default = False

            val = best_effort_python_val(python_type)

        params[param_name] = (python_type, val, default)

    import_statements = {
        LatchFile: "from latch.types import LatchFile",
        LatchDir: "from latch.types import LatchDir",
        enum.Enum: "from enum import Enum",
    }

    import_types = []
    enum_defs = []
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
                enum_defs.append(_enum_literal)

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
        for e in enum_defs:
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
                _, type_repr = _get_code_literal(best_effort_python_val(list_t), list_t)

            collection_literal += "]"
            return collection_literal, f"typing.List[{type_repr}]"

    return python_val, python_type
