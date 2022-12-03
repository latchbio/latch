import json
import os
import platform
import re
import sys
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from traceback import print_exc, walk_tb
from types import TracebackType
from typing import Dict, List, Optional, Tuple, Type

from latch_cli.constants import FILE_MAX_SIZE
from latch_cli.utils import get_local_package_version

# TODO (kenny) - use this constant everywhere where pkg_root is None
_IMPORT_CWD = os.path.abspath(os.getcwd())


class _CrashHandler:

    """Display and store useful information when the program fails

    * Display tracebacks
    * Parse and display opaque flytekit serialization error messages
    * Write necessary information to reproduce failure to a tarball
    """

    IGNORE_REGEX = re.compile(
        "(\.git|\.latch_report\.tar\.gz|traceback\.txt|metadata\.json)"
    )

    def __init__(self):

        self.metadata = {
            "latch_version": self.version,
            "platform": platform.system(),
            "os_name": os.name,
            "os_version": platform.version(),
        }

    @property
    def version(self) -> str:
        return get_local_package_version()

    def _write_state_to_tarball(self, pkg_path: Optional[str] = None):
        """Bundles files needed to reproduce failed state into a tarball.

        Tarball contains:
          * JSON holding platform + package version metadata
          * logs directory holding docker build logs
          * Text file holding traceback
          * (If register) workflow package (python files, Dockerfile, etc.)
        """

        tarball_path = ".latch_report.tar.gz"
        if os.path.exists(tarball_path):
            os.remove(tarball_path)

        with tarfile.open(tarball_path, mode="x:gz") as tf:

            # If calling stack frame is handling an exception, we want to store
            # the traceback in a log file.
            if sys.exc_info()[0] is not None:
                with tempfile.NamedTemporaryFile("w+") as ntf:
                    print_exc(file=ntf)
                    ntf.seek(0)
                    tf.add(ntf.name, arcname="traceback.txt")

            if pkg_path is not None:

                if os.path.exists(pkg_path + ".logs/"):
                    tf.add(pkg_path + "logs/", arcname="logs")

                pkg_files = [
                    os.path.join(dp, f)
                    for dp, _, filenames in os.walk(pkg_path)
                    for f in filenames
                    if not (self.IGNORE_REGEX.match(os.path.join(dp, f)))
                ]
                for file_path in pkg_files:
                    file_size = os.path.getsize(file_path)
                    if file_size < FILE_MAX_SIZE:
                        tf.add(file_path)
                    else:
                        with tempfile.NamedTemporaryFile("wb+") as ntf:
                            ntf.write(f"# first 4 MB of {file_path}\n".encode("utf-8"))
                            with open(file_path, "rb") as f:
                                stuff = f.read(FILE_MAX_SIZE)
                                ntf.write(stuff)
                                ntf.seek(0)
                            tf.add(ntf.name, arcname=file_path)

            with tempfile.NamedTemporaryFile("w+") as ntf:
                json.dump(self.metadata, ntf)
                ntf.seek(0)
                tf.add(ntf.name, arcname="metadata.json")

            print("\n>> Crash report written to .latch_report.tar.gz <<")

    def init(self, message: Optional[str], pkg_path: Optional[str] = None):
        """Custom error handling.

        When an exception is thrown:
            - Display an optional message
            - Pretty print the traceback
            - Parse unintuitive errors and redisplay
            - Store useful system information in a tarball for debugging
        """

        def _excepthook(
            type_: Type[BaseException],
            value: BaseException,
            traceback: Optional[TracebackType],
        ) -> None:
            print(f"{message} - printing traceback:\n")
            Traceback(
                type_,
                value,
                traceback,
            ).pretty_print()
            self._write_state_to_tarball(pkg_path)

        sys.excepthook = _excepthook


@dataclass
class Frame:
    filename: str
    lineno: int
    name: str
    locals_: dict


@dataclass
class _SyntaxError:
    filename: str
    lineno: int
    offset: int
    text: str
    end_lineno: int
    end_offset: int


class FlytekitException:
    ...


_CODE_CACHE: Dict[Tuple[str, int], List[str]] = {}


@dataclass
class Stack:
    exc_type: str
    exc_value: str
    is_cause: bool = False
    syntax_error: Optional[_SyntaxError] = None
    flytekit_exc: Optional[FlytekitException] = None
    frames: List[Frame] = field(default_factory=list)

    def pretty_print(self, max_frames: int = 2):
        def read_code_span(frame: Frame, line_interval: int = 3) -> str:

            span: List[str] = []
            cache_hit = _CODE_CACHE.get((frame.filename, frame.lineno))

            if cache_hit:
                return cache_hit

            with open(frame.filename, "rt", encoding="utf-8") as code_file:
                for i, line in enumerate(code_file.readlines()):
                    if i in range(
                        frame.lineno - line_interval, frame.lineno + line_interval + 1
                    ):
                        span.append((i + 1, line))

            _CODE_CACHE[(frame.filename, frame.lineno)] = span
            return span

        render_full_idx = len(self.frames) - max_frames
        for i, frame in enumerate(self.frames):

            if i < render_full_idx:
                print(f"{frame.filename}:{frame.lineno} in {frame.name}")
                if i > 0 and i == render_full_idx - 1:
                    print(f"{i} frames collapsed...")
            else:
                code_span = read_code_span(frame)
                print()
                print("--Traceback (most recent call last)--")
                print(f"{frame.filename}:{frame.lineno} in {frame.name}")

                for lineno, line in code_span:
                    if lineno == frame.lineno:
                        print(f"> {lineno}| {line}", end="")
                    else:
                        print(f"  {lineno}| {line}", end="")


class Traceback:
    def __init__(self, exc_type, exc_value, traceback):
        self.stacks = self.extract_stack(exc_type, exc_value, traceback)

    def extract_stack(
        self,
        exc_type: Type[BaseException],
        exc_value: BaseException,
        traceback: Optional[TracebackType],
    ) -> List[Stack]:

        stacks: List[Stack] = []
        while True:

            stack = Stack(
                exc_type=exc_type.__name__,
                exc_value=exc_value,
            )
            stacks.append(stack)

            # catch flytekit errors and parse for stuff
            if isinstance(exc_value, SyntaxError):
                stack.syntax_error = _SyntaxError(
                    offset=exc_value.offset or 0,
                    filename=exc_value.filename or "?",
                    lineno=exc_value.lineno or 0,
                    line=exc_value.text or "",
                    msg=exc_value.msg,
                )
            elif isinstance(exc_value, FlytekitException):
                stack.flytekit_exc = FlytekitException()

            for frame_summary, line_no in walk_tb(traceback):

                filename = frame_summary.f_code.co_filename
                # Frames with <frozen importlib._bootstrap> have no real debugging
                # information and just add noise to the traceback.
                if filename.startswith("<"):
                    continue
                if filename:
                    if not os.path.isabs(filename):
                        filename = os.path.join(_IMPORT_CWD, filename)
                frame = Frame(
                    filename=filename or "?",
                    lineno=line_no,
                    name=frame_summary.f_code.co_name,
                    locals_={
                        key: value for key, value in frame_summary.f_locals.items()
                    },
                )
                stack.frames.append(frame)

            # Explicitly chained - raise ... from
            # https://peps.python.org/pep-3134/#motivation
            cause = getattr(exc_value, "__cause__", None)
            if cause:
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

        return stacks

    def pretty_print(self):
        for i, stack in enumerate(self.stacks):
            stack.pretty_print()
            print(f"\n{stack.exc_type}: {stack.exc_value}")

            if i < len(self.stacks) - 1:
                if stack.is_cause:
                    print(
                        "\nThe above exception was the direct cause of the following exception:\n"
                    )
                else:
                    print(
                        "\nDuring handling of the above exception, another exception occurred:\n"
                    )


CrashHandler = _CrashHandler()
