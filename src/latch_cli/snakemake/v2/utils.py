from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any

from latch.types.directory import LatchDir
from latch.types.file import LatchFile


def get_config_val(val: Any):
    if isinstance(val, list):
        return [get_config_val(x) for x in val]
    if isinstance(val, dict):
        return {k: get_config_val(v) for k, v in val.items()}
    if isinstance(val, (LatchFile, LatchDir)):
        if val.remote_path is not None:
            return val.remote_path

        return str(val.path)
    if isinstance(val, (int, float, bool, type(None))):
        return val
    if is_dataclass(val):
        return {f.name: get_config_val(getattr(val, f.name)) for f in fields(val)}
    if isinstance(val, Enum):
        return val.value

    return str(val)
