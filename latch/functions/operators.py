"""
Mimics channel operators from Nextflow, using the correspondence Channel --> Python Dictionary
"""

import re
from itertools import product
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


def _combine(item1: Any, item2: Any):
    """
    Combines two items for use in *_join functions. The rules followed are:

    - If both items are lists, the lists are concatenated
    - If one of the items is a list, the other is appended to that list
    - Otherwise, the output is a new list containing both items

    This is so that composition of joins works as expected. We also use list
    addition so as to not modify the input items and instead return a new copy.
    """

    if isinstance(item1, List) and isinstance(item2, List):
        return item1 + item2
    elif isinstance(item1, List):
        return item1 + [item2]
    elif isinstance(item2, List):
        return [item1] + item2
    else:
        return [item1, item2]


def left_join(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    """A standard left join of two dictionaries, joining on their keys"""
    output = {}
    for key in left:
        if key in right:
            output[key] = _combine(left[key], right[key])
        else:
            output[key] = left[key]
    return output


def right_join(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    """A standard right join of two dictionaries, joining on their keys"""
    output = {}
    for key in right:
        if key in left:
            output[key] = _combine(left[key], right[key])
        else:
            output[key] = right[key]
    return output


def inner_join(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    """A standard inner join of two dictionaries, joining on their keys"""
    output = {}
    for key in left:
        if key in right:
            output[key] = _combine(left[key], right[key])
    return output


def outer_join(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
    """A standard outer join of two dictionaries, joining on their keys"""
    output = {}
    for key in left:
        if key in right:
            output[key] = _combine(left[key], right[key])
        else:
            output[key] = left[key]
    for key in right:
        if key not in output:
            output[key] = right[key]
    return output


def group_tuple(channel: List[Tuple], key_index: Optional[int] = None) -> List[Tuple]:
    output = {}
    if key_index is None:
        key_index = 0
    for element in channel:
        key = element[key_index]
        if key not in output:
            output[key] = tuple(
                [[k] if i == key_index else k for i, k in enumerate(element)]
            )
        else:
            for i, k in enumerate(element):
                if i == key_index:
                    continue
                output[key][i].append(k)
    return output


def latch_filter(channel: List[Any], predicate) -> List[Any]:
    if isinstance(predicate, Callable):
        return list(filter(predicate, channel))
    elif isinstance(predicate, re.Pattern):

        def filter_func(list_item: Any):
            if isinstance(list_item, str):
                return predicate.match(list_item)
            return False

        return list(filter(filter_func, channel))
    elif isinstance(predicate, type):

        def filter_func(list_item: Any):
            return isinstance(list_item, predicate)

        return list(filter(filter_func, channel))
    else:
        return channel


def combine(
    channel_0: List[Any],
    channel_1: List[Any],
    by: Optional[int] = None,
) -> Union[List, Dict[str, List[Any]]]:
    if by is not None:
        output = {}
        for element in channel_0:
            if element[by] not in output:
                output[element[by]] = [[], []]
            output[element[by]][0].append(element[:by] + element[by + 1 :])
        for element in channel_1:
            if element[by] not in output:
                output[element[by]] = [[], []]
            output[element[by]][1].append(element[:by] + element[by + 1 :])
        for key in output:
            output[key] = list(product(*output[key]))
        return output
    return list(product(channel_0, channel_1))
