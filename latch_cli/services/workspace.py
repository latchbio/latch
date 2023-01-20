from typing import List

import click

from latch_cli.config.latch import config
from latch_cli.config.user import user_config
from latch_cli.tinyrequests import post
from latch_cli.tui import select_tui
from latch_cli.utils import current_workspace, retrieve_or_login


def workspace():
    """Opens a terminal user interface in which a user can select the workspace
    the want to switch to.

    Like `get_executions`, this function should only be called from the CLI.
    """

    headers = {"Authorization": f"Bearer {retrieve_or_login()}"}

    resp = post(
        url=config.api.user.list_workspaces,
        headers=headers,
    )

    resp.raise_for_status()

    options = []
    data = resp.json()
    ids = {}

    for id, name in data.items():
        ids[name] = id
        options.append(name)

    selected_option = select_tui(
        title="Select Workspace",
        options=options,
    )

    if not selected_option:
        return

    new_id = ids[selected_option]

    old_id = current_workspace()
    if old_id != new_id:
        user_config.update_workspace(new_id)
        click.secho(f"Successfully switched to context {selected_option}", fg="green")
    else:
        click.secho(f"Already in context {selected_option}.", fg="green")
