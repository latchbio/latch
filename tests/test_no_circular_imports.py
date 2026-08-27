"""Guard against circular-import regressions between `latch` and `latch_cli`.

Several `latch_cli` modules import the `latch` package at load time, while the
`latch` package imports back into `latch_cli.utils` during its own
initialization. When a low-level module such as `latch_cli.utils` is imported
before `latch`, the two packages can meet on a partially-initialized module and
raise ``ImportError: cannot import name ... (most likely due to a circular
import)``.

The existing suite hides this because ``conftest`` imports a module that pulls
in `latch` first, priming the safe order. Each module below must instead import
cleanly as the *first* import in a fresh interpreter, regardless of order.
"""

import os
import subprocess
import sys

import pytest

# `latch_cli.utils` is the heart of the cycle; the rest are representative leaves
# that transitively pull it in. `docker.utils` and `private_images` back
# `latch image`, the surface the 2.77.0 report came from.
COLD_IMPORT_MODULES = [
    "latch",
    "latch_cli.utils",
    "latch_cli.utils.path",
    "latch_cli.services.docker.utils",
    "latch_cli.services.private_images",
    "latch_cli.exceptions.handler",
]


@pytest.mark.parametrize("module", COLD_IMPORT_MODULES)
def test_module_imports_in_a_fresh_interpreter(module: str) -> None:
    """Import `module` as the first import in a clean interpreter."""
    # Mirror this process's resolution order (src ahead of site-packages) so the
    # child imports the same package tree the suite runs against.
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, (
        f"`import {module}` failed in a fresh interpreter:\n{result.stderr}"
    )
