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
    """
    Operator to mimic the `groupTuple` construct from Nextflow:

        The `groupTuple` operator collects tuples (or lists) of values emitted
        by the source channel grouping together the elements that share the same
        key. Finally it emits a new tuple object for each distinct key collected.

    Args:
        channel: A list of tuples to be grouped by key_index
        key_index: Which index of the tuple to match against - if not provided,
            defaults to 0

    Example:
        >>> channel = [(1,'A'), (1,'B'), (2,'C'), (3, 'B'), (1,'C'), (2, 'A'), (3, 'D')]
        >>> group_tuple(channel) # key_index defaults to grouping by the first element (index 0)
        [(1, ['A', 'B', 'C']), (2, ['C', 'A']), (3, 'B', 'D')]
    """

    output = {}
    if key_index is None:
        key_index = 0
    for element in channel:
        if not (0 <= key_index < len(element)):
            raise ValueError(f"Key Index {key_index} too large for element {element}")
        key = element[key_index]
        if key not in output:
            output[key] = tuple(
                [[k] if i != key_index else k for i, k in enumerate(element)]
            )
        else:
            for i, k in enumerate(element):
                if i == key_index:
                    continue
                output[key][i].append(k)
    return list(output.values())


def latch_filter(
    channel: List[Any],
    predicate: Union[Callable, re.Pattern, type, None],
) -> List[Any]:
    """Filters a given list with either a predicate, a regex, or a type"""
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
    """
    Creates a Cartesian product of the two provided channels, with the option to first group
    the elements of the channels by a certain index and then doing individual products on the
    groups.

    Args:
        channel_0: A list. If by is provided, all elements must be tuples of the same length
        channel_1: A list. If by is provided, all elements must be tuples of the same length
            as channel_0
        by: If provided, which index to group by first.

    Example:
        >>> c0 = ['hello', 'ciao']
        >>> c1 = [1, 2, 3]
        >>> combine(c0, c1)
        [('hello', 1), ('hello', 2), ('hello', 3), ('ciao', 1), ('ciao', 2), ('ciao', 3)]
    """
    if by is not None:
        output = {}
        for element in channel_0:
            if not isinstance(element, tuple):
                raise ValueError(f"`by` is provided, but {element} is not a tuple.")
            if not (0 <= by < len(element)):
                raise ValueError(f"Combine index {by} too large for element {element}")
            if element[by] not in output:
                output[element[by]] = [[], []]
            output[element[by]][0].append(element[:by] + element[by + 1 :])
        for element in channel_1:
            if not isinstance(element, tuple):
                raise ValueError(f"`by` is provided, but {element} is not a tuple.")
            if not (0 <= by < len(element)):
                raise ValueError(f"Combine index {by} too large for element {element}")
            if element[by] not in output:
                output[element[by]] = [[], []]
            output[element[by]][1].append(element[:by] + element[by + 1 :])
        final_output = []
        for key in output:
            prod = list(product(*output[key]))
            for p1, p2 in prod:
                final_output.append((key,) + p1 + p2)
        return final_output
    return list(product(channel_0, channel_1))
