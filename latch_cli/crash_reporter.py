import os
import platform


class CrashReporter:

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
        PKG_NAME = "latch"
        try:
            from importlib import metadata
        except ImportError:
            import importlib_metadata as metadata
        return metadata.version(PKG_NAME)
