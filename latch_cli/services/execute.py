"""Service to execute a workflow in a container."""

from pathlib import Path
from typing import List

from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteEntities
from scp import SCPClient

from latch_cli.centromere.ctx import CentromereCtx
from latch_cli.centromere.utils import TmpDir
from latch_cli.services.utils import import_flyte_objects


def execute(local_script: Path):
    """Executes tasks and workflows on remote servers in their containers."""

    wd = Path().absolute()
    with CentromereCtx(
        wd,
        disable_auto_version=False,
        remote=True,
    ) as ctx:

        import_flyte_objects(wd)
        for entity in FlyteEntities.entities:
            if isinstance(entity, PythonTask):
                print(entity.name)

        # nucleus: get latest version associated with task name and later allow
        # version passed

        # then recover container - want to recover container from idl
        # definition of task and fallback to default workflow container

        #  taskname opt[version] -> image

        def run_script_in_container(
            ctx: CentromereCtx, image_name: str, script_name: str, remote_tmp_dir: Path
        ) -> List[str]:

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

        with TmpDir(ssh_client=ctx.ssh_client, remote=True) as td:

            scp = SCPClient(ctx.ssh_client.get_transport(), sanitize=lambda x: x)
            scp.put(str(local_script.resolve()), td)

            logs, container_id = run_script_in_container(
                ctx,
                "812206152185.dkr.ecr.us-west-2.amazonaws.com/4107_bulk-rnaseq:1.0.5-498ac7",
                local_script.name,
                td,
            )
            for x in logs:
                print(x, end="")
