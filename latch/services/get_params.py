
try:
    from typing import get_args
except ImportError:
    from typing_extensions import get_args

import json
import typing
from enum import Enum
from typing import Union

import google.protobuf.json_format as gpjson
from flyteidl.core.types_pb2 import LiteralType as _LiteralType
from flytekit.models.types import LiteralType
from latch.services.execute import _get_workflow_interface
from latch.types import LatchDir, LatchFile
from latch.utils import retrieve_or_login


def get_params(wf_name: Union[None, str], wf_version: Union[None, str] = None):
    """Constructs a python parameter map from a workflow name and (opt) version.

    Args:
        wf_name: The unique name of a workflow.
        wf_version: Workflow version
    """

    token = retrieve_or_login()
    wf_id, wf_interface = _get_workflow_interface(token, wf_name, wf_version)

    params = {}
    wf_vars = wf_interface["variables"]
    for key, value in wf_vars.items():
        try:
            description_json = json.loads(value['description'])
            param_name = description_json['name']
        except (json.decoder.JSONDecodeError, KeyError) as e:
            raise ValueError(
                f'Parameter description json for workflow {wf_name} is malformed') from e

        literal_type_json = value["type"]
        literal_type = gpjson.ParseDict(literal_type_json, _LiteralType())

        python_type = _guess_python_type(
            LiteralType.from_flyte_idl(literal_type))
        val = _best_effort_default_val(python_type)
        params[param_name] = (python_type, val)

    import_statements = {
        LatchFile: "from latch.types import LatchFile",
        LatchDir: "from latch.types import LatchDir",
        Enum: "from enum import Enum"
    }

    import_types = []
    param_map_str = ""
    param_map_str += '\nparams = {'
    param_map_str += f'\n    "_name": "{wf_name}", # Dont edit this value.'
    for param_name, value in params.items():
        python_type, val = value

        if python_type in import_statements and python_type not in import_types:
            import_types.append(python_type)

        if type(val) is str:
            val = f'"{val}"'
        param_map_str += f'\n    "{param_name}": {val}, # {python_type}'
    param_map_str += '\n}'

    with open(f'{wf_name}.params.py', 'w') as f:

        f.write(
            f'"""Run `latch execute {wf_name}.params.py` to execute this workflow"""\n')

        for t in import_types:
            f.write(f'\n{import_statements[t]}')
        f.write('\n')
        f.write(param_map_str)


class Unsupported:
    ...


def _guess_python_type(literal: LiteralType):
    """Transform flyte type literal to native python type."""

    if literal.simple is not None:
        simple_table = {
            0: None,
            1: int,
            2: float,
            3: str,
            4: bool,
            5: Unsupported,
            6: Unsupported,
            7: Unsupported,
            8: Unsupported,
            9: Unsupported,
        }
        return simple_table[literal.simple]

    if literal.collection_type is not None:
        return typing.List[_guess_python_type(literal.collection_type)]

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

    if literal.sum is not None:

        summand_types = [_guess_python_type(
            summand) for summand in literal.sum.summands]

        # Trying to directly construct set of types will throw error if list is
        # included as 'list' is not hashable.
        unique_summands = []
        for t in summand_types:
            if t not in unique_summands:
                unique_summands.append(t)

        return typing.Union[tuple(summand_types)]

    raise NotImplementedError(
        f"The flyte literal {literal} cannot be transformed to a python type.")


def _best_effort_default_val(t: typing.T):
    """Produce a "best-effort" default value given a python type."""

    primitive_table = {
        None: None,
        int: 0,
        float: 0.0,
        str: "foo",
        bool: False,
    }
    if t in primitive_table:
        return primitive_table[t]

    if t is list:
        return []

    file_like_table = {
        LatchDir: LatchDir("latch:///foobar"),
        LatchFile: LatchFile("latch:///foobar")
    }
    if t in file_like_table:
        return file_like_table[t]

    if not hasattr(t, '__origin__'):
        raise NotImplementedError(
            f"Unable to produce a best-effort value for the python type {t}")

    if t.__origin__ is list:
        list_args = get_args(t)
        if len(list_args) == 0:
            return []
        return [_best_effort_default_val(arg) for arg in list_args]

    if t.__origin__ is typing.Union:
        return _best_effort_default_val(get_args(t)[0])

    raise NotImplementedError(
        f"Unable to produce a best-effort value for the python type {t}")
