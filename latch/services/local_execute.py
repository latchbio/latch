"""Service to execute a workflow in a container."""

from pathlib import Path

import docker

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

    try:
        _fetch_logs = ctx.dkr_client.get_image(ctx.full_image_tagged)
        set(_fetch_logs)
    except docker.errors.APIError as e:
        raise ValueError(
            "Unable to find an image for"
            f"{ctx.full_image_tagged} locally. \n\nPlease register"
            " this workflow before attempting to run locally."
            f"\n\t $latch register {pkg_root}"
        ) from e

    _exec_cmd = ["python3", "/root/wf/__init__.py"]
    container = ctx.dkr_client.create_container(
        ctx.full_image_tagged,
        command=_exec_cmd,
    )
    container_id = container.get("Id")
    ctx.dkr_client.start(container_id)
    logs = ctx.dkr_client.logs(container_id, stream=True)

    print("\tStreaming stdout from registered container running locally")
    for x in logs:
        o = x.decode("utf-8")
        print(f"\t\t{o}")
