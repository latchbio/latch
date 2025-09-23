import json
import os
import sys
import webbrowser
from pathlib import Path

import click
import gql
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine, TypeTransformerFailedError
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.models.interface import Variable
from google.protobuf.json_format import MessageToJson

from latch.utils import current_workspace
from latch_cli import menus
from latch_cli.centromere.utils import import_flyte_objects
from latch_sdk_gql.execute import execute


def preview(pkg_root: Path):
    """Generate a preview of the parameter interface for a workflow.

    This will allow a user to see how their parameter interface will look
    without having to first register their workflow.

    Args:
        pkg_root: A valid path pointing to the worklow code a user wishes to
            preview. The path can be absolute or relative.

    Example:
        >>> preview("alphafold_wf")
    """

    try:
        modules = import_flyte_objects([pkg_root.resolve()])
        wfs: dict[str, PythonFunctionWorkflow] = {}
        for module in modules:
            for flyte_obj in module.__dict__.values():
                if isinstance(flyte_obj, PythonFunctionWorkflow):
                    wfs[flyte_obj.name] = flyte_obj

        if len(wfs) == 0:
            click.secho(f"Unable to find a workflow definition in {pkg_root}", fg="red")
            raise click.exceptions.Exit(1)
    except ImportError as e:
        click.secho(
            f"Unable to find {e.name} - make sure that all necessary packages"
            " are installed and you have the correct function name.",
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    wf = next(iter(wfs.values()))
    if len(wfs) > 1:
        choice = _select_workflow_tui(
            title="Select which workflow to preview", options=list(wfs.keys())
        )
        if choice is None:
            raise click.Abort

        wf = wfs[choice]

    ctx = FlyteContextManager.current_context()
    assert ctx is not None

    for param_name, x in wf.python_interface.inputs_with_defaults.items():
        if not isinstance(x, tuple):
            continue

        typ, default = x
        try:
            literal = TypeEngine.to_literal(
                ctx, default, typ, TypeEngine.to_literal_type(typ)
            )
        except TypeTransformerFailedError:
            continue

        cur = wf.interface.inputs[param_name]

        desc = json.loads(cur.description)
        desc["default"] = json.loads(MessageToJson(literal.to_flyte_idl()))

        wf.interface.inputs[param_name] = Variable(cur.type, json.dumps(desc))

    execute(
        gql.gql("""
            mutation UpdatePreview($accountId: BigInt!, $inputs: String!) {
                upsertWorkflowPreview(
                    input: { argAccountId: $accountId, argInputs: $inputs }
                ) {
                    clientMutationId
                }
            }
        """),
        {
            "accountId": current_workspace(),
            "inputs": MessageToJson(wf.interface.to_flyte_idl().inputs),
        },
    )

    webbrowser.open("https://console.latch.bio/preview/parameters")


# TODO(ayush): abstract this logic in a unified interface that all tui commands use
def _select_workflow_tui(title: str, options: list[str], clear_terminal: bool = True):
    """Renders a terminal UI that allows users to select one of the options listed in `options`

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

        menus._print(title)
        menus.line_down(2)

        num_lines_rendered = 4  # 4 "extra" lines for header + footer

        for i in range(start_index, start_index + max_per_page):
            if i >= len(options):
                break
            name = options[i]
            if i == curr_selected:
                color = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                menus._print(f"{indent}{color}{bold}{name}{reset}\x1b[1E")
            else:
                menus._print(f"{indent}{name}\x1b[1E")
            num_lines_rendered += 1

        menus.line_down(1)

        control_str = "[ARROW-KEYS] Navigate\t[ENTER] Select\t[Q] Quit"
        menus._print(control_str)
        menus.line_up(num_lines_rendered - 1)

        menus._show()

        return num_lines_rendered

    import termios
    import tty

    old_settings = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin.fileno())

    curr_selected = 0
    start_index = 0
    _, term_height = os.get_terminal_size()
    menus.remove_cursor()

    if not clear_terminal:
        _, curs_height = menus.current_cursor_position()
        max_per_page = term_height - curs_height - 4
    else:
        menus.clear_screen()
        menus.move_cursor((0, 0))
        max_per_page = term_height - 4

    num_lines_rendered = render(
        curr_selected, start_index=start_index, max_per_page=max_per_page
    )

    try:
        while True:
            b = menus.read_bytes(1)
            if b == b"\r":
                return options[curr_selected]
            elif b == b"\x1b":
                b = menus.read_bytes(2)
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
            menus.clear(num_lines_rendered)
            num_lines_rendered = render(
                curr_selected, start_index=start_index, max_per_page=max_per_page
            )
    except KeyboardInterrupt:
        ...
    finally:
        menus.clear(num_lines_rendered)
        menus.reveal_cursor()
        menus._show()
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)
