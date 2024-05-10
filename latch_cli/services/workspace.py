from typing import List, TypedDict

import click
from latch_sdk_config.user import user_config

from latch.utils import current_workspace, get_workspaces, WSInfo
from latch_cli.menus import SelectOption, select_tui


def workspace():
    """Opens a terminal user interface in which a user can select the workspace
    the want to switch to.

    Like `get_executions`, this function should only be called from the CLI.
    """
    data = get_workspaces()

    old_id = current_workspace()

    selected_marker = "\x1b[3m\x1b[2m (currently selected) \x1b[22m\x1b[23m"

    options: List[SelectOption[WSInfo]] = []
    for _, info in sorted(
        data.items(), key=lambda x: "" if x[1]["default"] else x[1]["name"]
    ):
        options.append(
            {
                "display_name": info["name"] if old_id != info["workspace_id"] else info["name"] + selected_marker,
                "value": info,
            }
        )

    selected_option = select_tui(
        title="Select Workspace",
        options=options,
        clear_terminal=False,
    )

    if selected_option is None:
        return

    user_config.update_workspace(selected_option["workspace_id"], selected_option["name"])

    if old_id != selected_option["workspace_id"]:
        click.secho(
            f"Successfully switched to context {selected_option['name']}", fg="green"
        )
    else:
        click.secho(f"Already in context {selected_option['name']}.", fg="green")
