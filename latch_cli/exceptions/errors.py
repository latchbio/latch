from dataclasses import dataclass
from typing import Union


@dataclass
class _SyntaxError(BaseException):
    filename: str
    lineno: int
    offset: int
    text: str
    end_lineno: int
    end_offset: int


class _FlytekitError(BaseException): ...


_HandledError = Union[_SyntaxError, _FlytekitError]
