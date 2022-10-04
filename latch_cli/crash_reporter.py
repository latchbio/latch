import json
import os
import platform
import sys
import tarfile
import tempfile
import traceback
from typing import Optional

from latch_cli.constants import FILE_MAX_SIZE
from latch_cli.utils import get_local_package_version


class _CrashReporter:

    """Write logs + system information to disk when Exception is thrown."""

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

    def report(self, pkg_path: Optional[str] = None):
        """Constructs a zipped tarball with files needed to reproduce crashes."""

        tarball_path = ".latch_report.tar.gz"
        if os.path.exists(tarball_path):
            os.remove(tarball_path)

        # Tarball contains:
        #   * JSON holding platform + package version metadata
        #   * logs directory holding docker build logs
        #   * Text file holding traceback
        #   * (If register) workflow package (python files, Dockerfile, etc.)
        with tarfile.open(tarball_path, mode="x:gz") as tf:

            # If calling stack frame is handling an exception, we want to store
            # the traceback in a log file.
            if sys.exc_info()[0] is not None:
                with tempfile.NamedTemporaryFile("w+") as ntf:
                    traceback.print_exc(file=ntf)
                    ntf.seek(0)
                    tf.add(ntf.name, arcname="traceback.txt")

            if pkg_path is not None:

                if os.path.exists(pkg_path + ".logs/"):
                    tf.add(pkg_path + "logs/", arcname="logs")

                pkg_files = [
                    os.path.join(dp, f)
                    for dp, _, filenames in os.walk(pkg_path)
                    for f in filenames
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


CrashReporter = _CrashReporter()
