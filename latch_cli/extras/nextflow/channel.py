import json
from typing import Dict, List, Type, TypeVar

from latch.types.metadata import _IsDataclass

T = TypeVar("T", bound=_IsDataclass)


def get_mapper_inputs(
    cls: Type[T],
    wf_inputs: Dict[str, object],
    channel_inputs: Dict[str, str],
) -> List[T]:
    value_channels = {}
    queue_channels = {}

    min_len = float("inf")
    for param_name, channel in channel_inputs.items():
        values = json.loads(channel)

        if type(values) == list:
            queue_channels[param_name] = values
            min_len = min(min_len, len(values))
        else:
            value_channels[param_name] = values

    if min_len == float("inf"):
        min_len = 1

    res: List[T] = []
    for i in range(min_len):
        kwargs = {**wf_inputs, **value_channels}

        for k, v in queue_channels.items():
            kwargs[k] = v[i]

        res.append(cls(**kwargs))

    return res
