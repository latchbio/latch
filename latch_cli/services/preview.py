import os
import sys
import termios
import tty
import webbrowser
from pathlib import Path
from typing import List

from flytekit.core.workflow import PythonFunctionWorkflow
from google.protobuf.json_format import MessageToJson
from latch_sdk_config.latch import config

import latch_cli.tui as tui
from latch_cli.centromere.utils import _import_flyte_objects
from latch_cli.tinyrequests import post
from latch_cli.utils import current_workspace, retrieve_or_login


# TODO(ayush): make this import the `wf` directory and use the package root
# instead of the workflow name. also redo the frontend, also make it open the
# page
def preview(pkg_root: Path):
    """Generate a preview of the parameter interface for a workflow.

    This will allow a user to see how their parameter interface will look
    without having to first register their workflow.

    Args:
        pkg_root: A valid path pointing to the worklow code a user wishes to
            preview. The path can be absolute or relative.

    Example:
        >>> preview("wf.__init__.alphafold_wf")
    """

    try:
        modules = _import_flyte_objects([pkg_root.resolve()])
        wfs: dict[str, PythonFunctionWorkflow] = {}
        for module in modules:
            for flyte_obj in module.__dict__.values():
                if isinstance(flyte_obj, PythonFunctionWorkflow):
                    wfs[flyte_obj.name] = flyte_obj
        if len(wfs) == 0:
            raise ValueError(f"Unable to find a workflow definition in {pkg_root}")
    except ImportError as e:
        raise ValueError(
            f"Unable to find {e.name} - make sure that all necessary packages"
            " are installed and you have the correct function name."
        )

    wf = list(wfs.values())[0]
    if len(wfs) > 1:
        wf = wfs[
            _select_workflow_tui(
                title="Select which workflow to preview",
                options=list(wfs.keys()),
            )
        ]

    resp = post(
        url=config.api.workflow.preview,
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "workflow_ui_preview": MessageToJson(wf.interface.to_flyte_idl().inputs),
            "ws_account_id": current_workspace(),
        },
    )

    resp.raise_for_status()

    url = f"{config.console_url}/preview/parameters"
    webbrowser.open(url)


# TODO(ayush): abstract this logic in a unified interface that all tui commands use
def _select_workflow_tui(title: str, options: List[str], clear_terminal: bool = True):
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
