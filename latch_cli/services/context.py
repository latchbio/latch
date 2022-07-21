from pathlib import Path

import click

from latch_cli.config.latch import LatchConfig
from latch_cli.tinyrequests import post
from latch_cli.tui import tui_select
from latch_cli.utils import retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def context():
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}

    resp = post(url=endpoints["get-ws"], headers=headers)

    resp.raise_for_status()

    options = ["Personal Workspace"]
    data = resp.json()
    ids = {"Personal Workspace": "0"}

    for id, name in data.items():
        ids[name] = id
        options.append(name)

    selected_option = tui_select(
        title="Select Workspace", options=options, clear_terminal=False
    )

    if not selected_option:
        return

    new_id = ids[selected_option]
    context_file = Path.home() / ".latch" / "context"
    context_file.touch(exist_ok=True)

    with open(context_file, "r") as f:
        old_id = f.read()
    if old_id != new_id:
        with open(context_file, "w") as f:
            f.write(ids[selected_option])
        click.secho(f"Successfully switched to context {selected_option}", fg="green")
    else:
        click.secho(f"Already in context {selected_option}.", fg="green")
