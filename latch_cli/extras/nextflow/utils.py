from pathlib import Path

from .types import NextflowInputParamType, NextflowOutputParamType, NextflowParamType


def format_param_name(name: str, t: NextflowParamType):
    if name == "-":
        return "stdout"
    if t in {NextflowInputParamType.path, NextflowOutputParamType.fileoutparam}:
        return Path(name).stem
    if t == NextflowOutputParamType.tupleoutparam:
        return name.replace("<", "_").replace(">", "_")
    return name
