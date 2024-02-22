from typing import Callable, TypeVar
from warnings import warn

from typing_extensions import ParamSpec

T = TypeVar("T")
P = ParamSpec("P")


_DEPRECATION_VERSION = "3.0.0"


def _deprecated_import(
    name: str,
    new_import_source: str,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(f: Callable[P, T]) -> Callable[P, T]:
        warn(
            (
                f"Importing `{name}` directly from `latch` is deprecated, and will"
                f" be removed in version {_DEPRECATION_VERSION}.\n\n    Please use"
                f" the full import `from {new_import_source} import {name}`\n"
            ),
            DeprecationWarning,
        )

        return f

    return decorator


def _deprecated() -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(f: Callable[P, T]) -> Callable[P, T]:
        warn(
            (
                f"{f.__name__} is deprecated, and will be removed in version"
                f" {_DEPRECATION_VERSION}."
            ),
            DeprecationWarning,
        )

        return f

    return decorator
