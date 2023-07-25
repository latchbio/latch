import json
import os
import platform
import sys
import tarfile
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from traceback import print_exc
from types import TracebackType
from typing import List, Optional, Type

import click

from latch_cli.constants import latch_constants
from latch_cli.exceptions.traceback import _Traceback
from latch_cli.utils import get_local_package_version


@dataclass(frozen=True)
class _Metadata:
    os_name: str = os.name
    os_version: str = platform.version()
    latch_version: str = get_local_package_version()
    py_version: str = sys.version
    platform: str = platform.platform()

    def print(self):
        click.secho("Crash info:", fg="red", bold=True)
        click.echo(
            " ".join(
                [
                    click.style("Latch SDK version:", fg="red"),
                    self.latch_version,
                ]
            )
        )
        click.echo(
            " ".join(
                [
                    click.style("Python version:", fg="red"),
                    self.py_version.replace("\n", ";"),
                ]
            )
        )
        click.echo(" ".join([click.style("Platform:", fg="red"), self.platform]))
        click.echo(
            " ".join(
                [
                    click.style("OS:", fg="red"),
                    f"{self.os_name}; {self.os_version}",
                ]
            )
        )


class CrashHandler:
    """Display and store useful information when the program fails

    * Display tracebacks
    * Parse and display opaque flytekit serialization error messages
    * Write necessary information to reproduce failure to a tarball
    """

    def __init__(self):
        self.metadata: _Metadata = _Metadata()
        self.message: Optional[str] = None
        self.pkg_root: Optional[str] = None

    def _write_state_to_tarball(self):
        """Bundles files needed to reproduce failed state into a tarball.

        Tarball contains:
          * JSON holding platform + package version metadata
          * logs directory holding docker build logs
          * Text file holding traceback
          * (If register) workflow package (python files, Dockerfile, etc.)
        """

        tarball_path = Path(".latch_report.tar.gz").resolve()
        tarball_path.unlink(missing_ok=True)

        with tarfile.open(tarball_path, mode="x:gz") as tf:
            # If calling stack frame is handling an exception, we want to store
            # the traceback in a log file.
            if sys.exc_info()[0] is not None:
                with tempfile.NamedTemporaryFile("w+") as ntf:
                    print_exc(file=ntf)
                    ntf.seek(0)
                    tf.add(ntf.name, arcname="traceback.txt")

            if self.pkg_root is not None:
                logs_path = Path(self.pkg_root) / ".latch" / ".logs"
                if logs_path.exists():
                    tf.add(logs_path, arcname="logs")

                pkg_files: List[Path] = []
                for dp, _, fnames in os.walk(logs_path):
                    for f in fnames:
                        p = (Path(dp) / f).resolve()
                        if (
                            latch_constants.ignore_regex.search(str(p))
                            or p.is_symlink()
                            or p.stat().st_size > latch_constants.file_max_size
                        ):
                            continue

                        pkg_files.append(p)

                for file_path in pkg_files:
                    tf.add(file_path)

            with tempfile.NamedTemporaryFile("w+") as ntf:
                json.dump(asdict(self.metadata), ntf)
                ntf.seek(0)
                tf.add(ntf.name, arcname="metadata.json")

        print(f"Crash report written to {tarball_path}")

    def init(self):
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
            click.secho(f"\n{self.message}:\n", fg="red", bold=True)
            _Traceback(type_, value, traceback).pretty_print()

            self.metadata.print()

            if os.environ.get("LATCH_NO_CRASH_REPORT") == "1":
                click.secho(
                    "Not generating crash report due to $LATCH_NO_CRASH_REPORT",
                    bold=True,
                )
                return

            if not click.confirm("Generate a crash report?"):
                return

            print("Generating...")
            self._write_state_to_tarball()

        sys.excepthook = _excepthook
