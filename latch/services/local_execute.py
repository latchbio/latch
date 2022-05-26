"""Service to execute a workflow in a container."""

from pathlib import Path

import docker

from latch.services.register import RegisterCtx, build_image


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
        print("\tSpinning up local container...")
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
    except:
        print(
            "\tUnable to find a local image associated with the local"
            " workflow version."
        )
        _manual_build(ctx)
        local_execute(pkg_root)


def _print_build_logs(build_logs, image):
    print(f"\tBuilding Docker image for {image}")
    for x in build_logs:
        line = x.get("stream")
        error = x.get("error")
        if error is not None:
            print(f"\t\t{x}")
            raise OSError(f"Error when building image ~ {x}")
        elif line is not None:
            print(f"\t\t{line}", end="")


def _manual_build(ctx: RegisterCtx):
    dockerfile = ctx.pkg_root.joinpath("Dockerfile")
    build_logs = build_image(ctx, dockerfile)
    _print_build_logs(build_logs, ctx.full_image_tagged)
