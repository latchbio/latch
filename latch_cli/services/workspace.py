from typing import List, TypedDict

import click
from latch_sdk_config.user import user_config

from latch.utils import current_workspace, get_workspaces
from latch_cli.menus import SelectOption, select_tui


class WSInfo(TypedDict):
    workspace_id: str
    name: str


def workspace():
    """Opens a terminal user interface in which a user can select the workspace
    the want to switch to.

    Like `get_executions`, this function should only be called from the CLI.
    """
    data = get_workspaces()

    old_id = current_workspace()

    selected_marker = "\x1b[3m\x1b[2m (currently selected) \x1b[22m\x1b[23m"

    options: List[SelectOption[WSInfo]] = []
    for id, name in sorted(
        data.items(), key=lambda x: "" if x[1] == "Personal Workspace" else x[0]
    ):
        display_name = name
        if id == old_id:
            display_name = f"{name}{selected_marker}"

        options.append({
            "display_name": display_name,
            "value": {
                "workspace_id": id,
                "name": name,
            },
        })

    selected_option = select_tui(
        title="Select Workspace",
        options=options,
        clear_terminal=False,
    )

    if selected_option is None:
        return

    user_config.update_workspace(**selected_option)

    if old_id != selected_option["workspace_id"]:
        click.secho(
            f"Successfully switched to context {selected_option['name']}", fg="green"
        )
    else:
        click.secho(f"Already in context {selected_option['name']}.", fg="green")
