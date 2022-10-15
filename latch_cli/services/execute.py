"""Service to execute a workflow in a container."""

from inspect import getmembers
from pathlib import Path
from typing import List, Optional

from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteEntities
from scp import SCPClient

from latch_cli.centromere.ctx import CentromereCtx
from latch_cli.centromere.utils import TmpDir, import_flyte_objects


def run_script_in_container(
    ctx: CentromereCtx, image_name: str, script_name: str, remote_tmp_dir: Path
) -> (List[str], str):

    _serialize_cmd = ["python3", script_name]
    container = ctx.dkr_client.create_container(
        image_name,
        command=_serialize_cmd,
        volumes=[str(remote_tmp_dir)],
        host_config=ctx.dkr_client.create_host_config(
            binds={
                str(remote_tmp_dir): {
                    "bind": "/root",
                    "mode": "rw",
                },
            }
        ),
    )
    container_id = container.get("Id")
    ctx.dkr_client.start(container_id)
    logs = ctx.dkr_client.logs(container_id, stream=True)

    return [x.decode("utf-8") for x in logs], container_id


def execute(local_script: Path):
    """Executes tasks and workflows on remote servers in their containers."""

    # TODO - verify this is root of workflow directory
    wd = Path().absolute()
    with CentromereCtx(
        wd,
        disable_auto_version=False,
        remote=True,
    ) as ctx:

        mod = import_flyte_objects([wd, local_script.parent], local_script.stem)[0]

        found_task = False
        task_name: Optional[str] = None
        for name, o in getmembers(mod):
            if isinstance(o, PythonTask):
                if found_task:
                    raise ValueError(
                        "Identified more than one task in the "
                        "script provided to execute. Please provide "
                        "a single task per script as assigning a "
                        "container is ambiguous otherwise. "
                    )
                task_name = name
                found_task = True

        if task_name is None:
            raise ValueError("Could not identify valid task in provided script.")

        import_flyte_objects([wd])
        found: Optional[str] = None
        for entity in FlyteEntities.entities:
            if isinstance(entity, PythonTask):
                if task_name in entity.name.split(".")[-1]:
                    found = entity.name  # fqn of registered task
                    break

        if not found:
            raise ValueError(
                f"The task provided in your script {task_name} is not defined "
                "in the 'wf' package in your working directory."
            )

        # image_name = ctx.nucleus_get_image(task_name, version=None)
        image_name = (
            "812206152185.dkr.ecr.us-west-2.amazonaws.com/4107_bulk-rnaseq:1.0.5-498ac7"
        )

        with TmpDir(ssh_client=ctx.ssh_client, remote=True) as td:

            scp = SCPClient(ctx.ssh_client.get_transport(), sanitize=lambda x: x)
            scp.put(str(local_script.resolve()), td)

            logs, container_id = run_script_in_container(
                ctx,
                image_name,
                local_script.name,
                td,
            )
            for x in logs:
                print(x, end="")
