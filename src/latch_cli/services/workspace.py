from typing import Optional

import click

from latch.utils import (
    NoWorkspaceSelectedError,
    WSInfo,
    current_workspace,
    get_workspaces,
)
from latch_cli.menus import SelectOption, select_tui
from latch_sdk_config.user import user_config


def workspace(workspace_id: Optional[str] = None):
    """Opens a terminal user interface in which a user can select the workspace they want to switch to.

    Like `get_executions`, this function should only be called from the CLI.
    """
    data = get_workspaces()

    old_id: str | None
    try:
        old_id = current_workspace()
    except NoWorkspaceSelectedError:
        old_id = None

    if workspace_id is not None:
        selected_option = data.get(workspace_id)
        if selected_option is None:
            click.secho(
                f"Workspace {workspace_id} does not exist or you do not have permission to access it.",
                fg="red",
                bold=True,
            )
            raise click.exceptions.Exit(1)

        user_config.update_workspace(
            selected_option["workspace_id"], selected_option["name"]
        )

        if old_id != selected_option["workspace_id"]:
            click.secho(
                f"Successfully switched to context {selected_option['name']}",
                fg="green",
            )
        else:
            click.secho(f"Already in context {selected_option['name']}.", fg="green")
        return

    selected_marker = "\x1b[3m\x1b[2m (currently selected) \x1b[22m\x1b[23m"

    options: list[SelectOption[WSInfo]] = []
    for _, info in sorted(
        data.items(), key=lambda x: "" if x[1]["default"] else x[1]["name"]
    ):
        options.append(
            {
                "display_name": (
                    info["name"]
                    if old_id != info["workspace_id"]
                    else info["name"] + selected_marker
                ),
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
