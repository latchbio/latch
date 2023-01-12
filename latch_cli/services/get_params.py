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
    build_python_literal,
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

    import_statements: typing.Dict[str, typing.List[str]] = {
        "latchfile": ["from latch.types import LatchFile"],
        "latchdir": ["from latch.types import LatchDir"],
        "enum": ["from enum import Enum"],
        "class": [
            "from dataclasses import dataclass",
            "from dataclasses_json import dataclass_json",
        ],
    }
    imported_statements: typing.Dict[str, bool] = {}

    enum_defs = []
    class_defs = []

    def _add_imports(type_: typing.T):
        if type(type_) is LatchFile and "latchfile" not in imported_statements:
            import_statements["latchfile"] = True
            # LatchFile can also be a dataclass
            return

        if type(type_) is LatchDir and "latchdir" not in imported_statements:
            import_statements["latchdir"] = True
            # LatchDir can also be a dataclass
            return

        if type(type_) is enum.EnumMeta and "enum" not in imported_statements:
            import_statements["enum"] = True
            return

        if (
            "__dataclass_fields__" in type_.__dict__
            and "class" not in imported_statements
        ):
            import_statements["class"] = True

    def _define_enums(type_: any):

        if type(type_) is not enum.EnumMeta:
            return

        variants = type_._variants
        name = type_._name

        _enum_def_literal = f"class {name}(Enum):"
        for variant in variants:
            if variant in keyword.kwlist:
                variant_name = f"_{variant}"
            else:
                variant_name = variant
            _enum_def_literal += f"\n    {variant_name} = '{variant}'"
        enum_defs.append(_enum_def_literal)

    def _define_classes(type_: any):

        if "__dataclass_fields__" not in type_.__dict__:
            return

        _class_def_literal = f"@dataclass_json\n@dataclass\nclass {type_.__name__}():"
        for field in type_.__dict__["__dataclass_fields__"].values():
            _class_def_literal += f"\n    {field.name}: {field.type}"
        class_defs.append(_class_def_literal)

    param_map_str = ""
    param_map_str += "\nparams = {"
    param_map_str += f'\n    "_name": "{wf_name}", # Don\'t edit this value.'

    for param_name, value in params.items():
        python_type, python_val, default = value

        def _walk_type(_type: typing.T):
            if get_origin(_type) is not None:
                if get_origin(_type) is list:
                    _walk_type(get_args(_type)[0])
                elif get_origin(_type) is typing.Union:
                    for variant in get_args(_type):
                        _walk_type(variant)
            elif "__dataclass_fields__" in _type.__dict__:
                _add_imports(_type)
                _define_enums(_type)
                _define_classes(_type)
                for field in _type.__dict__["__dataclass_fields__"].values():
                    _walk_type(field.type)
            else:
                _add_imports(_type)
                _define_enums(_type)
                _define_classes(_type)

        _walk_type(python_type)
        python_val, python_type = build_python_literal(python_val, python_type)

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

        for k in imported_statements:
            f.write(f"\n{import_statements[k]}")
        for e in enum_defs:
            f.write(f"\n\n{e}\n")
        for c in class_defs:
            f.write(f"\n\n{c}\n")

        f.write("\n")
        f.write(param_map_str)
