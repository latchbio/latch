import os
import sys
import termios
import tty
from pathlib import Path
from typing import List

import click

import latch_cli.tui as tui
from latch_cli.config.latch import LatchConfig
from latch_cli.config.user import UserConfig
from latch_cli.tinyrequests import post
from latch_cli.utils import current_workspace, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def workspace():
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}

    resp = post(
        url=endpoints["get-ws"],
        headers=headers,
    )

    resp.raise_for_status()

    options = []
    data = resp.json()
    ids = {}

    for id, name in data.items():
        ids[name] = id
        options.append(name)

    selected_option = select_workspace_tui(
        title="Select Workspace",
        options=options,
    )

    if not selected_option:
        return

    new_id = ids[selected_option]
    context_file = Path.home() / ".latch" / "context"
    context_file.touch(exist_ok=True)

    old_id = current_workspace()
    if old_id != new_id:
        user_conf = UserConfig()
        user_conf.update_workspace(new_id)
        click.secho(f"Successfully switched to context {selected_option}", fg="green")
    else:
        click.secho(f"Already in context {selected_option}.", fg="green")


def select_workspace_tui(title: str, options: List[str], clear_terminal: bool = True):
    """
    Renders a terminal UI that allows users to select one of the options
    listed in `options`

    Args:
        title: The title of the selection window.
        options: A list of names for each of the options.
        clear_terminal: Whether or not to clear the entire terminal window
            before displaying - default False
    """

    if len(options) == 0:
        raise ValueError("No options given")

    def render(
        curr_selected: int,
        start_index: int = 0,
        max_per_page: int = 10,
        indent: str = "    ",
    ) -> int:
        if curr_selected < 0 or curr_selected >= len(options):
            curr_selected = 0

        tui._print(title)
        tui.line_down(2)

        num_lines_rendered = 4  # 4 "extra" lines for header + footer

        for i in range(start_index, start_index + max_per_page):
            if i >= len(options):
                break
            name = options[i]
            if i == curr_selected:
                color = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                tui._print(f"{indent}{color}{bold}{name}{reset}\x1b[1E")
            else:
                tui._print(f"{indent}{name}\x1b[1E")
            num_lines_rendered += 1

        tui.line_down(1)

        control_str = "[ARROW-KEYS] Navigate\t[ENTER] Select\t[Q] Quit"
        tui._print(control_str)
        tui.line_up(num_lines_rendered - 1)

        tui._show()

        return num_lines_rendered

    old_settings = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin.fileno())

    curr_selected = 0
    start_index = 0
    _, term_height = os.get_terminal_size()
    tui.remove_cursor()

    if not clear_terminal:
        _, curs_height = tui.current_cursor_position()
        max_per_page = term_height - curs_height - 4
    else:
        tui.clear_screen()
        tui.move_cursor((0, 0))
        max_per_page = term_height - 4

    num_lines_rendered = render(
        curr_selected,
        start_index=start_index,
        max_per_page=max_per_page,
    )

    try:
        while True:
            b = tui.read_bytes(1)
            if b == b"\r":
                return options[curr_selected]
            elif b == b"\x1b":
                b = tui.read_bytes(2)
                if b == b"[A":  # Up Arrow
                    curr_selected = max(curr_selected - 1, 0)
                    if (
                        curr_selected - start_index < max_per_page // 2
                        and start_index > 0
                    ):
                        start_index -= 1
                elif b == b"[B":  # Down Arrow
                    curr_selected = min(curr_selected + 1, len(options) - 1)
                    if (
                        curr_selected - start_index > max_per_page // 2
                        and start_index < len(options) - max_per_page
                    ):
                        start_index += 1
                else:
                    continue
            tui.clear(num_lines_rendered)
            num_lines_rendered = render(
                curr_selected,
                start_index=start_index,
                max_per_page=max_per_page,
            )
    except KeyboardInterrupt:
        ...
    finally:
        tui.clear(num_lines_rendered)
        tui.reveal_cursor()
        tui._show()
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)
