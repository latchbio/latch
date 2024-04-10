try:
    from typing import get_args, get_origin
except ImportError:
    from typing_extensions import get_args, get_origin

import enum
import json
import keyword
import typing
from typing import Optional

import google.protobuf.json_format as gpjson
from flyteidl.core.literals_pb2 import Literal as _Literal
from flyteidl.core.types_pb2 import LiteralType as _LiteralType
from flytekit.models.literals import Literal
from flytekit.models.types import LiteralType

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.utils import retrieve_or_login
from latch_cli.services.launch import _get_workflow_interface


class _Unsupported: ...


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
            description_json = json.loads(value["description"])
            param_name = description_json["name"]
        except (json.decoder.JSONDecodeError, KeyError) as e:
            raise ValueError(
                f"Parameter description json for workflow {wf_name} is malformed"
            ) from e

        literal_type_json = value["type"]
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


def _guess_python_type(literal: LiteralType, param_name: str):
    """Transform flyte type literal to native python type."""

    if literal.simple is not None:
        return _simple_table[literal.simple]

    if literal.collection_type is not None:
        return typing.List[_guess_python_type(literal.collection_type, param_name)]

    if literal.blob is not None:
        # flyteidl BlobType message for reference:
        #   enum BlobDimensionality {
        #       SINGLE = 0;
        #       MULTIPART = 1;
        #   }

        dim = literal.blob.dimensionality
        if dim == 0:
            return LatchFile
        else:
            return LatchDir

    if literal.union_type is not None:
        variant_types = [
            _guess_python_type(variant, param_name)
            for variant in literal.union_type.variants
        ]

        # Trying to directly construct set of types will throw error if list is
        # included as 'list' is not hashable.
        unique_variants = []
        for t in variant_types:
            if t not in unique_variants:
                unique_variants.append(t)

        return typing.Union[tuple(variant_types)]

    if literal.enum_type is not None:
        # We can hold the variants a proxy class that is also type 'Enum', s.t.
        # we can parse the variants and define the object in the param map
        # code.

        class _VariantCarrier(enum.Enum): ...

        _VariantCarrier._variants = literal.enum_type.values
        # Use param name to uniquely identify each enum
        _VariantCarrier._name = param_name
        return _VariantCarrier

    raise NotImplementedError(
        f"The flyte literal {literal} cannot be transformed to a python type."
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
