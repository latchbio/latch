import json
import os
import platform
import sys
import tarfile
import tempfile
from pathlib import Path
from traceback import print_exc, walk_tb
from types import TracebackType
from typing import Optional, Type

from latch_cli.constants import FILE_MAX_SIZE, IGNORE_REGEX
from latch_cli.exceptions.traceback import _Traceback
from latch_cli.utils import get_local_package_version


class CrashHandler:
    """Display and store useful information when the program fails

    * Display tracebacks
    * Parse and display opaque flytekit serialization error messages
    * Write necessary information to reproduce failure to a tarball
    """

    def __init__(self):
        self.metadata = {
            "latch_version": get_local_package_version(),
            "platform": platform.system(),
            "os_name": os.name,
            "os_version": platform.version(),
        }

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
                pkg_path = Path(self.pkg_root).resolve()

                if (pkg_path / ".logs").exists():
                    tf.add(pkg_path / ".logs", arcname="logs")

                pkg_files = [
                    Path(dp) / f
                    for dp, _, filenames in os.walk(pkg_path)
                    for f in filenames
                    if not IGNORE_REGEX.match(str(Path(dp) / f))
                ]
                for file_path in pkg_files:
                    if os.path.getsize(file_path) < FILE_MAX_SIZE:
                        tf.add(file_path)
                    else:
                        with tempfile.NamedTemporaryFile("wb+") as ntf:
                            ntf.write(f"# first 4 MB of {file_path}\n".encode("utf-8"))
                            with open(file_path, "rb") as f:
                                ntf.write(f.read(FILE_MAX_SIZE))
                                ntf.seek(0)
                            tf.add(ntf.name, arcname=file_path)

            with tempfile.NamedTemporaryFile("w+") as ntf:
                json.dump(self.metadata, ntf)
                ntf.seek(0)
                tf.add(ntf.name, arcname="metadata.json")

            print("\n>> Crash report written to .latch_report.tar.gz <<")

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
            print(f"{self.message} - printing traceback:\n")
            _Traceback(type_, value, traceback).pretty_print()
            self._write_state_to_tarball()

        sys.excepthook = _excepthook
