from dataclasses import dataclass, field
from pathlib import Path
from traceback import walk_tb
from types import TracebackType
from typing import List, Optional, Type

from latch_cli.exceptions.cache import _code_cache
from latch_cli.exceptions.errors import _FlytekitError, _HandledError, _SyntaxError


@dataclass
class _Frame:
    filename: str
    lineno: int
    name: str
    locals_: dict

    def read(self, line_interval: int = 3) -> str:
        cache_hit = _code_cache.get((self.filename, self.lineno))
        if cache_hit is not None:
            return cache_hit

        span: List[str] = []
        with open(self.filename, "r") as code_file:
            for i, line in enumerate(code_file.readlines()):
                if i in range(
                    self.lineno - line_interval,
                    self.lineno + line_interval + 1,
                ):
                    span.append((i + 1, line))

        _code_cache[(self.filename, self.lineno)] = span

        return span


@dataclass
class _Stack:
    exc_type: str
    exc_value: str
    is_cause: bool = False
    error: Optional[_HandledError] = None
    frames: List[_Frame] = field(default_factory=list)

    def pretty_print(self, max_frames: int = 20):
        render_full_idx = len(self.frames) - max_frames
        for i, frame in enumerate(self.frames):
            if i < render_full_idx:
                print(f"{frame.filename}:{frame.lineno} in {frame.name}")
                if i > 0 and i == render_full_idx - 1:
                    print(f"{i} frames collapsed...")
            else:
                print()
                print("--Traceback (most recent call last)--")
                print(f"{frame.filename}:{frame.lineno} in {frame.name}")

                for lineno, line in frame.read():
                    if lineno == frame.lineno:
                        print(f"> {lineno}| {line}", end="")
                    else:
                        print(f"  {lineno}| {line}", end="")


class _Traceback:
    def __init__(self, exc_type, exc_value, traceback):
        self.stacks = self.extract_stack(exc_type, exc_value, traceback)

    def extract_stack(
        self,
        exc_type: Type[BaseException],
        exc_value: BaseException,
        traceback: Optional[TracebackType],
    ) -> List[_Stack]:
        stacks: List[_Stack] = []
        while True:
            stack = _Stack(
                exc_type=exc_type.__name__,
                exc_value=exc_value,
            )
            stacks.append(stack)

            if isinstance(exc_value, SyntaxError):
                stack.error = _SyntaxError(
                    filename=exc_value.filename or "?",
                    lineno=exc_value.lineno or 0,
                    offset=exc_value.offset or 0,
                    text=exc_value.text or "",
                    end_lineno=exc_value.end_lineno or 0,
                    end_offset=exc_value.end_offset or 0,
                )
            elif isinstance(exc_value, _FlytekitError):
                stack.error = _FlytekitError()

            for frame_summary, line_no in walk_tb(traceback):
                filename = frame_summary.f_code.co_filename

                # Frames with <frozen importlib._bootstrap> have no real debugging
                # information and just add noise to the traceback.
                if filename.startswith("<"):
                    continue
                if filename:
                    filename = str(Path(filename).resolve())

                stack.frames.append(
                    _Frame(
                        filename=filename or "?",
                        lineno=line_no,
                        name=frame_summary.f_code.co_name,
                        locals_=frame_summary.f_locals.copy(),
                    )
                )

            # Explicitly chained - raise ... from
            # https://peps.python.org/pep-3134/#motivation
            cause = getattr(exc_value, "__cause__", None)
            if cause is not None:
                stack.is_cause = True
                exc_type = cause.__class__
                exc_value = cause
                traceback = cause.__traceback__
                continue

            # Implicity chained
            context = exc_value.__context__
            if context and not getattr(exc_value, "__suppress_context__", False):
                stack.is_cause = False
                exc_type = context.__class__
                exc_value = context
                traceback = context.__traceback__
                continue

            break

        return stacks[::-1]

    def pretty_print(self):
        for i, stack in enumerate(self.stacks):
            stack.pretty_print()
            print(f"\n{stack.exc_type}: {stack.exc_value}")

            if i < len(self.stacks) - 1:
                if stack.is_cause:
                    print(
                        "\nThe above exception was the direct cause of the following"
                        " exception:\n"
                    )
                else:
                    print(
                        "\nDuring handling of the above exception, another exception"
                        " occurred:\n"
                    )
