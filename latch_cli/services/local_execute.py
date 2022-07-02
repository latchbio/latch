"""Service to execute a workflow in a container."""

from pathlib import Path

import docker.errors

from latch_cli.services.register import RegisterCtx, _print_build_logs, build_image


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

    dockerfile = ctx.pkg_root.joinpath("Dockerfile")

    def _create_container(image_name: str):
        container = ctx.dkr_client.create_container(
            image_name,
            command=["python3", "/root/wf/__init__.py"],
            volumes=[str(ctx.pkg_root)],
            host_config=ctx.dkr_client.create_host_config(
                binds={
                    str(ctx.pkg_root): {
                        "bind": "/root",
                        "mode": "rw",
                    },
                }
            ),
        )
        return container

    try:
        print("Spinning up local container...")
        print("NOTE ~ workflow code is bound as a mount.")
        print("You must register your workflow to persist changes.")

        container = _create_container(ctx.full_image_tagged)

    except docker.errors.ImageNotFound as e:
        print("Unable to find an image associated to this version of your workflow")
        print("Building from scratch:")

        build_logs = build_image(ctx, dockerfile)
        _print_build_logs(build_logs, ctx.full_image_tagged)

        container = _create_container(ctx.full_image_tagged)

    container_id = container.get("Id")

    ctx.dkr_client.start(container_id)
    logs = ctx.dkr_client.logs(container_id, stream=True)
    for x in logs:
        o = x.decode("utf-8")
        print(o, end="")
