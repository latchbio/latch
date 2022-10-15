"""Service to execute a workflow in a container."""

import contextlib
from dataclasses import dataclass
from inspect import getmembers
from pathlib import Path
from typing import List, Optional

from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteEntities
from scp import SCPClient

from latch_cli.centromere.ctx import CentromereCtx
from latch_cli.centromere.utils import TmpDir, import_flyte_objects
from latch_cli.services.test_data.utils import _retrieve_creds


@dataclass
class Creds:
    secret_key: str
    session_token: str
    access_key: str


def run_script_in_container(
    ctx: CentromereCtx,
    image_name: str,
    script_name: str,
    remote_tmp_dir: str,
    remote_workflow_pkg_dir: str,
    creds: Creds,
) -> (List[str], str):

    # command=["python3", f"/tmp/{script_name}"],
    container = ctx.dkr_client.create_container(
        image_name,
        command=["python3", f"/tmp/{script_name}"],
        volumes=[remote_tmp_dir, remote_workflow_pkg_dir],
        host_config=ctx.dkr_client.create_host_config(
            binds={
                remote_tmp_dir: {
                    "bind": "/tmp",
                    "mode": "rw",
                },
                f"{remote_workflow_pkg_dir}/wf": {
                    "bind": "/root/wf",
                    "mode": "rw",
                },
            }
        ),
        environment={
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            "AWS_SESSION_TOKEN": creds.session_token,
            "AWS_DEFAULT_REGION": "us-west-2",
        },
    )
    container_id = container.get("Id")
    ctx.dkr_client.start(container_id)
    logs = ctx.dkr_client.logs(container_id, stream=True)

    return [x.decode("utf-8") for x in logs], container_id


def debug(local_script: Path):
    """Executes tasks and workflows on remote servers in their containers."""

    wd = Path().absolute()

    # assume i can recover the task name from some obvious place in my script
    # task_name = "wf.prepare_inputs.prepare_inputs"

    with CentromereCtx(
        wd,
        disable_auto_version=False,
        remote=True,
    ) as ctx:

        # image_name = ctx.nucleus_get_image(task_name, version=None)
        image_name = (
            "812206152185.dkr.ecr.us-west-2.amazonaws.com/4107_bulk-rnaseq:1.0.6-6c2c4b"
        )

        with contextlib.ExitStack() as stack:

            td = stack.enter_context(
                TmpDir(
                    ssh_client=ctx.ssh_client,
                    remote=True,
                )
            )

            wf_pkg = stack.enter_context(
                TmpDir(
                    ssh_client=ctx.ssh_client,
                    remote=True,
                )
            )

            scp = SCPClient(ctx.ssh_client.get_transport(), sanitize=lambda x: x)
            scp.put(str(local_script.resolve()), td)
            scp.put(str(ctx.pkg_root / "wf"), wf_pkg, recursive=True)

            session_token, access_key, secret_key, _ = _retrieve_creds()
            logs, container_id = run_script_in_container(
                ctx,
                image_name,
                local_script.name,
                td,
                wf_pkg,
                Creds(
                    secret_key=secret_key,
                    access_key=access_key,
                    session_token=session_token,
                ),
            )
            for x in logs:
                print(x, end="")


def old_debug(task_name: Path):
    """Executes tasks and workflows on remote servers in their containers."""

    # TODO - verify this is root of workflow directory
    wd = Path().absolute()

    with CentromereCtx(
        wd,
        disable_auto_version=False,
        remote=True,
    ) as ctx:

        # image_name = ctx.nucleus_get_image(task_name, version=None)
        image_name = (
            "812206152185.dkr.ecr.us-west-2.amazonaws.com/4107_bulk-rnaseq:1.0.5-498ac7"
        )

        with TmpDir(ssh_client=ctx.ssh_client, remote=True) as td:

            # scp = SCPClient(ctx.ssh_client.get_transport(), sanitize=lambda x: x)
            # scp.put(str(local_script.resolve()), td)

            container = ctx.dkr_client.create_container(
                image_name,
                command=["/bin/bash"],
                volumes=[str(td)],
                host_config=ctx.dkr_client.create_host_config(
                    binds={
                        str(td): {
                            "bind": "/root",
                            "mode": "rw",
                        },
                    }
                ),
                tty=True,
                stdin_open=True,
            )
            container_id = container.get("Id")
            ctx.dkr_client.start(container_id)
            logs = ctx.dkr_client.logs(container_id, stream=True)

            for x in logs:
                print(x, end="")
