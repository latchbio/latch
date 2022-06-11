"""Service to execute a workflow in a container."""

from pathlib import Path

from latch_cli.services.register import RegisterCtx, _print_build_logs


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
    wf_pkg = ctx.pkg_root.joinpath("wf")

    try:
        print("\tSpinning up local container...")
        print("\tNOTE ~ workflow code is bound as a mount.")
        print("\tYou must register your workflow to persist changes.")

        container = ctx.dkr_client.create_container(
            ctx.full_image_tagged,
            command=["python3", "/root/wf/__init__.py"],
            volumes=[str(wf_pkg)],
            host_config=ctx.dkr_client.create_host_config(
                binds={
                    str(wf_pkg): {
                        "bind": "/root/wf",
                        "mode": "rw",
                    },
                }
            ),
        )

        container_id = container.get("Id")
        ctx.dkr_client.start(container_id)

        logs = ctx.dkr_client.logs(container_id, stream=True)
        print("\n\tStreaming stdout from registered container running locally")
        for x in logs:
            o = x.decode("utf-8")
            print(f"\t\t{o}")
    except:
        print(
            "\tUnable to find a local image associated with the local"
            " workflow version."
        )
        build_logs = ctx.dkr_client.logs(container_id, stream=True)
        _print_build_logs(build_logs, ctx.full_image_tagged)
        local_execute(pkg_root)
