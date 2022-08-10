"""Service to execute a workflow in a container."""

from pathlib import Path

import docker.errors

from latch_cli.services.register import RegisterCtx, _print_build_logs, build_image
from flytekit.core.context_manager import FlyteContext, FlyteContextManager


def local_execute(
    pkg_root: Path, 
    use_auto_version: bool,
    output_dir: Path,
) -> None:
    """Executes a workflow locally within its latest registered container.

    Will stream in-container local execution stdout to terminal from which the
    subcommand is executed.

    Args:
        pkg_root: A path pointing to to the workflow package to be executed
            locally.
        use_auto_version: A bool indicating whether to use the default
            auto-versioning of the workflow. Recommended to be set to False for
            local execution so that previous images can be reused. Only really need
            to set to True for local execution if you update the Dockerfile.
        output_dir: The name of the output directory that the workflow writes to.
            In the workflow that means it will write to {output_dir}. These files will
            be visible in the 'outputs' folder locally.


    Example: ::

        $ latch local-execute myworkflow
        # Where `myworkflow` is a directory with workflow code.
    """

    ctx = RegisterCtx(pkg_root, disable_auto_version=(not use_auto_version))

    dockerfile = ctx.pkg_root.joinpath("Dockerfile")

    def _create_container(image_name: str):
        # Copy contents of workflow package to container's root directory to 
        # emulate natve workflow execution, rather than running from 
        # /root/local_execute, and rather than binding to /root.
        cmd = f"cp -r /root/local_execute/!({output_dir.stem}) /root ;" + \
              "python3 /root/wf/__init__.py"
        container = ctx.dkr_client.create_container(
            image_name,
            command=["bash", "-O", "extglob", "-c", cmd],
            volumes=[str(ctx.pkg_root)],
            host_config=ctx.dkr_client.create_host_config(
                binds={
                    str(ctx.pkg_root): {
                        "bind": "/root/local_execute",
                        "mode": "rw",
                    },
                    str(ctx.pkg_root.joinpath('outputs')): {
                        "bind": output_dir,
                        "mode": "rw",
                    },
                }
            ),
            working_dir="/root",
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
