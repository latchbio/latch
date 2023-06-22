import click
from latch_sdk_config.latch import config
from latch_sdk_config.user import user_config

from latch_cli.tinyrequests import post
from latch_cli.tui import select_tui
from latch_cli.utils import current_workspace, retrieve_or_login


def _get_workspaces():
    headers = {"Authorization": f"Bearer {retrieve_or_login()}"}

    resp = post(
        url=config.api.user.list_workspaces,
        headers=headers,
    )
    resp.raise_for_status()

    data = resp.json()
    return data


def workspace():
    """Opens a terminal user interface in which a user can select the workspace
    the want to switch to.

    Like `get_executions`, this function should only be called from the CLI.
    """
    options = []
    data = _get_workspaces()
    ids = {}

    for id, name in sorted(
        data.items(), key=lambda x: "" if x[1] == "Personal Workspace" else x[1]
    ):
        ids[name] = id
        options.append(name)

    selected_option = select_tui(
        title="Select Workspace",
        options=options,
    )

    if selected_option is None:
        return

    new_id = ids[selected_option]
    user_config.update_workspace(new_id, selected_option)

    old_id = current_workspace()
    if old_id != new_id:
        click.secho(f"Successfully switched to context {selected_option}", fg="green")
    else:
        click.secho(f"Already in context {selected_option}.", fg="green")
