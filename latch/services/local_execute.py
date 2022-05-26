"""Service to execute a workflow in a container."""

from pathlib import Path

from latch.services.register import RegisterCtx


def local_execute(pkg_root: Path):
    """Executes a workflow locally within its latest registered container.

    Will stream in-container local execution stdout to terminal from which the
    subcommand is executed.

    Args:
        pkg_root: A path pointing to to the workflow package to be executed
        locally.

    Example: ::

        $ latch local-execute myworkflow
        # Where `myworkflow` is a directory with workflow code.
    """

    ctx = RegisterCtx(pkg_root)

    print(ctx.full_image_tagged)
