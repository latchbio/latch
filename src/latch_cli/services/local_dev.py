import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from enum import Enum
from multiprocessing.managers import SyncManager
from pathlib import Path
from textwrap import dedent
from threading import Event
from typing import Dict, Optional

import click
from latch_sdk_config.latch import config

from latch.utils import current_workspace, retrieve_or_login
from latch_cli.constants import latch_constants
from latch_cli.menus import select_tui
from latch_cli.tinyrequests import post
from latch_cli.utils import TemporarySSHCredentials

max_polls = 1800


class TaskSize(str, Enum):
    small_task = "small_task"
    medium_task = "medium_task"
    large_task = "large_task"
    small_gpu_task = "small_gpu_task"
    large_gpu_task = "large_gpu_task"


human_readable_task_sizes: Dict[str, TaskSize] = {
    "Small Task": TaskSize.small_task,
    "Medium Task": TaskSize.medium_task,
    "Large Task": TaskSize.large_task,
    "Small GPU Task": TaskSize.small_gpu_task,
    "Large GPU Task": TaskSize.large_gpu_task,
}


def _get_latest_image(pkg_root: Path) -> str:
    ws_id = current_workspace()
    if int(ws_id) < 10:
        ws_id = f"x{ws_id}"

    registry_name = f"{ws_id}_{pkg_root.name}"

    resp = post(
        config.api.workflow.get_latest,
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "registry_name": registry_name,
            "ws_account_id": current_workspace(),
        },
    )

    try:
        resp.raise_for_status()
        latest_version = resp.json()["version"]
    except:
        raise ValueError(
            "There was an issue getting your workflow's docker image. Please make sure"
            " you've registered your workflow at least once."
        )

    return f"{config.dkr_repo}/{ws_id}_{pkg_root.name}:{latest_version}"


def rsync(pkg_root: Path, ip: str, ssh_command: str, cancel_event: Event):
    while True:
        if cancel_event.is_set():
            return

        subprocess.run(
            [
                "rsync",
                f"--rsh={ssh_command}",
                "--compress",
                "--recursive",
                "--links",
                "--times",
                "--devices",
                "--specials",
                f"{pkg_root}/",
                f"root@{ip}:/root/",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # todo(ayush): use inotify or something instead of running on a fixed interval
        time.sleep(0.5)


def local_development(
    pkg_root: Path,
    *,
    skip_confirm_dialog: bool = False,
    size: Optional[TaskSize] = None,
    image: Optional[str] = None,
):
    """Starts a REPL that allows a user to interactively run tasks to help with
    debugging during workflow development.

    In this REPL, you can run tasks or scripts and make edits to them without
    having to reregister your workflow. You can also get a shell into a
    container with the same environment as the one that the workflow runs in, to
    help debug installation issues. See the full documentation for `Local
    Development` for more info.

    Like `get_executions`, this should only be called from the CLI for best
    results.

    Args:
        pkg_root: A path that points to a valid workflow directory (see the
            docs for `register`)

    """

    # ensure that rsync is installed
    try:
        subprocess.run(
            ["rsync", "--version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    except FileNotFoundError:
        raise ValueError(dedent("""
                rsync is required for latch develop. Please install rsync and try again
                    linux: apt install rsync
                    mac: brew install rsync
                """))

    if image is None:
        image = _get_latest_image(pkg_root)

    if size is None:
        size = select_tui(
            "Please select an instance size",
            [
                {"display_name": k, "value": v}
                for k, v in human_readable_task_sizes.items()
            ],
        )
        if size is None:
            click.echo("Session cancelled.")
            return

    click.secho("Initiating local development session", fg="blue")
    click.echo(click.style("Selected image: ", fg="blue") + image)
    click.echo(click.style("Selected size: ", fg="blue") + size)

    if skip_confirm_dialog:
        click.echo("Proceeding without confirmation because of --yes")
    elif not click.confirm("Proceed?"):
        click.echo("Session cancelled.")
        return

    key_path = pkg_root / ".latch/ssh_key"
    jump_key_path = pkg_root / ".latch/jump_key"
    with TemporarySSHCredentials(key_path) as ssh:
        click.echo(
            "Starting local development session. This may take a few minutes for larger"
            " task sizes."
        )

        resp = post(
            "https://centromere.latch.bio/develop/start",
            headers={"Authorization": f"Latch-SDK-Token {retrieve_or_login()}"},
            json={
                "ImageName": image,
                "Workspace": current_workspace(),
                "SSHKey": ssh.public_key,
                "Size": size.value,
            },
        )

        json_data = resp.json()
        if resp.status_code != 200:
            raise ValueError(json_data["Error"])

        ip = json_data["IP"]
        jump_key = json_data["JumpKey"]

        jump_key_path.write_text(jump_key)
        jump_key_path.chmod(0o600)

        try:
            subprocess.run(
                ["ssh-add", str(jump_key_path)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            raise ValueError("Unable to add jump host key to SSH Agent") from e

        try:
            poll_count = 0
            while poll_count < max_polls:
                resp = post(
                    "https://centromere.latch.bio/develop/ready",
                    headers={"Authorization": f"Latch-SDK-Token {retrieve_or_login()}"},
                )

                if resp.status_code != 200:
                    raise ValueError(resp.json()["Error"])

                if resp.json()["Ready"]:
                    break

                time.sleep(1)
                poll_count += 1

            if poll_count == max_polls:
                raise ValueError(
                    "Unable to provision instance due to server load - "
                    "please try again later."
                )

            ssh_command = [
                "ssh",
                "-o",
                "CheckHostIP=no",  # hack
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "ServerAliveInterval=30",
                "-o",
                "ServerAliveCountMax=5",
                "-J",
                f"{latch_constants.jump_user}@{latch_constants.jump_host}",
                f"root@{ip}",
            ]

            with ProcessPoolExecutor() as exec:
                with SyncManager() as man:
                    stop_rsync = man.Event()
                    exec.submit(
                        rsync,
                        pkg_root,
                        ip,
                        " ".join(ssh_command[:-1]),
                        stop_rsync,
                    )

                    res = subprocess.run(ssh_command, stderr=subprocess.PIPE)
                    if "Too many authentication failures" in res.stderr.decode():
                        click.secho(
                            dedent("""
                            Too many authentication failures. Try resetting your ssh-agent with

                                $ ssh-add -D

                            and trying again."""),
                            fg="red",
                        )
                    stop_rsync.set()

        finally:
            resp = post(
                "https://centromere.latch.bio/develop/stop",
                headers={"Authorization": f"Latch-SDK-Token {retrieve_or_login()}"},
                json={"ImageName": image},
            )

            if resp.status_code != 200:
                raise ValueError(resp.json()["Error"])

            try:
                subprocess.run(
                    ["ssh-add", "-d", jump_key_path],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise ValueError("Unable to remove jump host key from SSH Agent")

            jump_key_path.unlink(missing_ok=True)
