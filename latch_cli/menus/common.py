import sys
from typing import Callable, Generic, Tuple

from git import Optional
from typing_extensions import TypedDict, TypeVar

old_print = print


def buffered_print() -> Tuple[Callable, Callable]:
    buffer = []

    def __print(*args):
        for arg in args:
            buffer.append(arg)

    def __show():
        nonlocal buffer
        sys.stdout.write("".join(buffer))
        sys.stdout.flush()

        buffer = []

    return __print, __show


# Allows for exactly one print per render, removing any weird flashing
# behavior and also speeding things up considerably
print, show = buffered_print()


T = TypeVar("T")


class SelectOption(TypedDict, Generic[T]):
    display_name: str
    value: T
