import os
import sys
import termios
import textwrap
import tty
from pathlib import Path
from typing import Dict, List

from apscheduler.schedulers.background import BackgroundScheduler

import latch_cli.tui as tui
from latch_cli.config.latch import config
from latch_cli.tinyrequests import post
from latch_cli.utils import account_id_from_token, current_workspace, retrieve_or_login


def get_executions():
    """Open an interactive terminal user interface that shows all executions for
    a particular user on Latch.

    This function in many ways mimics the interface at https://console.latch.bio
    but in a terminal. You may scroll around using either the arrow keys (or
    [HJKL] if you like Vim) and you can hit [ENTER] to see more information
    about the selected execution. You can see logs, and abort a running
    execution.

    This function should only be called from the CLI, as doing so in any other
    setting will likely cause the interface to behave in an unexpected way.
    """

    token = retrieve_or_login()
    context = current_workspace()
    headers = {"Authorization": f"Bearer {token}"}

    resp = post(
        url=config.api.execution.list,
        headers=headers,
        json={"ws_account_id": context},
    )

    resp.raise_for_status()

    options = []
    data: dict[str, dict] = resp.json()
    display_columns = [
        "display_name",
        "workflow_tagged",
        "start_time",
        "status",
    ]

    for execution_data in sorted(data.values(), key=lambda x: -int(x["id"])):
        options.append(
            {
                **execution_data,
                "workflow_tagged": f'{execution_data["workflow_name"]}/{execution_data["workflow_version"]}',
            }
        )

    _all_executions(
        title="All Executions",
        column_names=display_columns,
        options=options,
    )


def _all_executions(
    title: str,
    column_names: List[str],
    options: List[Dict[str, str]],
):
    if len(options) == 0:
        raise ValueError("No executions to show.")
    elif len(column_names) == 0:
        raise ValueError("No column names specified")

    term_width, term_height = os.get_terminal_size()

    def render(
        curr_selected: int,
        hor_index: int,
        term_width: int,
        term_height: int,
    ) -> int:
        # DISCLAIMER : MOST OF THE MAGIC NUMBERS HERE WERE THROUGH TRIAL AND ERROR

        tui.move_cursor((2, 2))

        max_per_page = term_height - 5
        vert_index = max(0, curr_selected - max_per_page // 2)

        lengths = {col: len(col) for col in column_names}
        for j in range(len(options)):
            values = options[j]
            for col in column_names:
                if values[col] is None:
                    values[col] = ""
                lengths[col] = max(lengths[col], len(values[col]))

        if len(column_names) > 1:
            column_spacing = max(
                2,
                (term_width - 7 - sum(lengths.values())) // (len(column_names) - 1),
            )
        else:
            column_spacing = 2

        max_row_len = sum(lengths.values()) + column_spacing * (len(column_names) - 1)

        tui._print(title)
        tui.line_down(2)

        for i in range(vert_index, vert_index + max_per_page):
            if i >= len(options):
                break
            values = options[i]
            row_str = ""
            for j, col in enumerate(column_names):
                item = values[col]
                if j == len(column_names) - 1:
                    row_str = row_str + f"{item: <{lengths[col]}}"
                else:
                    row_str = row_str + f"{item: <{lengths[col] + column_spacing}}"

            row_str = row_str[hor_index : hor_index + term_width - 6]

            if i == curr_selected:
                green = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                row_str = f"{green}{bold}{row_str}{reset}"

            tui.move_cursor_right(3)
            tui._print(row_str)
            tui.line_down(1)

        tui.move_cursor((2, term_height - 1))
        control_str = "[ARROW-KEYS] Navigate\t[ENTER] Select\t[Q] Quit"
        tui._print(control_str)
        tui.draw_box((2, 3), term_height - 5, term_width - 4)

        tui._show()
        return max_row_len

    old_settings = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin.fileno())

    curr_selected = hor_index = 0

    tui.remove_cursor()
    tui.clear_screen()
    tui.move_cursor((0, 0))

    prev = (curr_selected, hor_index, term_width, term_height)
    max_row_len = render(curr_selected, hor_index, term_width, term_height)

    try:
        while True:
            b = tui.read_bytes(1)
            term_width, term_height = os.get_terminal_size()
            rerender = False
            if b == b"\r":
                selected_execution_data = options[curr_selected]
                resp = post(
                    config.api.workflow.graph,
                    headers={"Authorization": f"Bearer {retrieve_or_login()}"},
                    json={
                        "workflow_id": selected_execution_data["workflow_id"],
                        "execution_id": selected_execution_data["id"],
                    },
                )
                _execution_dashboard(selected_execution_data, resp.json())
                rerender = True
            elif b == b"\x1b":
                b = tui.read_bytes(2)
                if b == b"[A":  # Up Arrow
                    curr_selected = max(0, curr_selected - 1)
                elif b == b"[B":  # Down Arrow
                    curr_selected = min(len(options) - 1, curr_selected + 1)
                elif b == b"[D":  # Left Arrow
                    if max_row_len > term_width + 7:
                        hor_index = max(0, hor_index - 5)
                elif b == b"[C":  # Right Arrow
                    if max_row_len > term_width + 7:
                        hor_index = min(max_row_len - term_width + 7, hor_index + 5)
                elif b == b"[1":  # Start of SHIFT + arrow keys
                    b = tui.read_bytes(3)
                    if b == b";2A":  # Up Arrow
                        curr_selected = max(0, curr_selected - 20)
                    elif b == b";2B":  # Down Arrow
                        curr_selected = min(len(options) - 1, curr_selected + 20)
                    elif b == b";2D":  # Left Arrow
                        if max_row_len > term_width + 7:
                            hor_index = max(0, hor_index - 25)
                    elif b == b";2C":  # Right Arrow
                        if max_row_len > term_width + 7:
                            hor_index = min(
                                max_row_len - term_width + 7, hor_index + 25
                            )
            elif b == b"k":
                curr_selected = max(curr_selected - 1, 0)
            elif b == b"j":
                curr_selected = min(curr_selected + 1, len(options) - 1)
            elif b == b"J":
                curr_selected = min(curr_selected + 20, len(options) - 1)
            elif b == b"K":
                curr_selected = max(curr_selected - 20, 0)
            elif b == b"h":
                if max_row_len > term_width + 7:
                    hor_index = max(0, hor_index - 5)
            elif b == b"l":
                if max_row_len > term_width + 7:
                    hor_index = min(max_row_len - term_width + 7, hor_index + 5)
            elif b == b"H":
                if max_row_len > term_width + 7:
                    hor_index = max(0, hor_index - 25)
            elif b == b"L":
                if max_row_len > term_width + 7:
                    hor_index = min(max_row_len - term_width + 7, hor_index + 25)
            if rerender or (
                (curr_selected, hor_index, term_width, term_height) != prev
            ):
                prev = (curr_selected, hor_index, term_width, term_height)
                tui.clear_screen()
                max_row_len = render(curr_selected, hor_index, term_width, term_height)
    except KeyboardInterrupt:
        ...
    finally:
        tui.clear_screen()
        tui.reveal_cursor()
        tui.move_cursor((0, 0))
        tui._show()
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)


def _execution_dashboard(execution_data: Dict[str, str], workflow_graph: Dict):
    fixed_workflow_graph = list(workflow_graph.items())

    def render(curr_selected: int, term_width: int, term_height: int):
        # DISCLAIMER : MOST OF THE MAGIC NUMBERS HERE WERE THROUGH TRIAL AND ERROR
        tui.move_cursor((2, 2))
        tui._print(
            f'{execution_data["display_name"]} - {execution_data["workflow_tagged"]}'
        )
        tui.draw_box((2, 3), term_height - 5, term_width - 4)

        tui.move_cursor((2, term_height - 1))
        instructions = [
            "[ARROW-KEYS] Navigate",
            "[ENTER] View Task Logs",
            # "[R] Relaunch",
        ]
        if execution_data["status"] == "RUNNING":
            instructions.append("[A] Abort")
        instructions.append("[Q] Back to All Executions")
        tui._print("\t".join(instructions))

        tui.move_cursor((4, 4))
        for i, (_, task) in enumerate(fixed_workflow_graph):
            name, status = task["name"] or task["sub_wf_name"], task["status"]
            row_str = "  ".join([name, status])
            if i == curr_selected:
                green = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                tui._print(f"{green}{bold}{row_str}{reset}")
            else:
                tui._print(row_str)
            tui.line_down(1)
            tui.move_cursor_right(3)
        tui._show()

    tui.clear_screen()

    try:
        term_width, term_height = os.get_terminal_size()
        curr_selected = 0

        render(curr_selected, term_width, term_height)
        prev = (curr_selected, term_width, term_height)
        while True:
            b = tui.read_bytes(1)
            rerender = False
            if b == b"\r":
                _log_window(execution_data, fixed_workflow_graph, curr_selected)
                rerender = True
            # elif b in (b"r", b"R"):
            #     relaunch_modal(execution_data)
            #     rerender = True
            elif b in (b"a", b"A"):
                _abort_modal(execution_data)
                rerender = True
            elif b == b"\x1b":
                b = tui.read_bytes(2)
                if b == b"[A":  # Up Arrow
                    curr_selected = max(curr_selected - 1, 0)
                elif b == b"[B":  # Down Arrow
                    curr_selected = min(curr_selected + 1, len(workflow_graph) - 1)
                elif b == b"[1":  # Start of SHIFT + arrow keys
                    b = tui.read_bytes(3)
                    if b == b";2A":  # Up Arrow
                        curr_selected = max(0, curr_selected - 20)
                    elif b == b";2B":  # Down Arrow
                        curr_selected += min(
                            curr_selected + 20, len(workflow_graph) - 1
                        )
            elif b == b"j":
                curr_selected = min(curr_selected + 1, len(workflow_graph) - 1)
            elif b == b"k":
                curr_selected = max(curr_selected - 1, 0)
            elif b == b"J":
                curr_selected = min(curr_selected + 20, len(workflow_graph) - 1)
            elif b == b"K":
                curr_selected = max(curr_selected - 20, 0)
            term_width, term_height = os.get_terminal_size()
            if rerender or (prev != (curr_selected, term_width, term_height)):
                tui.clear_screen()
                prev = (curr_selected, term_width, term_height)
                render(curr_selected, term_width, term_height)
    except KeyboardInterrupt:
        ...
    finally:
        tui.clear_screen()
        tui.move_cursor((0, 0))
        tui._show()


def _loading_screen(text: str):
    # DISCLAIMER : MOST OF THE MAGIC NUMBERS HERE WERE THROUGH TRIAL AND ERROR
    term_width, term_height = os.get_terminal_size()

    tui.clear_screen()
    tui.draw_box((2, 3), term_height - 5, term_width - 4)

    x = (term_width - len(text)) // 2
    y = term_height // 2

    tui.move_cursor((x, y))
    tui._print(text)
    tui._show()


def _log_window(execution_data, fixed_workflow_graph: list, selected: int):
    # DISCLAIMER : MOST OF THE MAGIC NUMBERS HERE WERE THROUGH TRIAL AND ERROR
    _loading_screen("Loading logs...")

    _, selected_task = fixed_workflow_graph[selected]

    log_file = Path(".latch_execution_log").resolve()
    log_file.touch(exist_ok=True)

    ws_id = current_workspace()
    if ws_id is None or ws_id == "":
        ws_id = account_id_from_token(retrieve_or_login())

    def get_logs():
        resp = post(
            config.api.execution.logs,
            headers={"Authorization": f"Bearer {retrieve_or_login()}"},
            json={
                "exec_id": execution_data["id"],
                "node_id": selected_task["node_id"],
                "account_id": ws_id,
            },
        )
        resp.raise_for_status()
        with open(log_file, "w") as f:
            f.write(resp.json()["message"].replace("\t", "    "))

    def render(vert_index, hor_index, term_width, term_height):
        tui.move_cursor((2, 2))
        tui._print(
            f'{execution_data["display_name"]} - {execution_data["workflow_tagged"]} -'
            f' {selected_task["name"]}'
        )
        tui.draw_box((2, 3), term_height - 5, term_width - 4)
        tui.move_cursor((4, 4))
        with open(log_file, "r") as f:
            for i, line in enumerate(f):
                if i < vert_index:
                    continue
                elif i > vert_index + term_height - 7:
                    continue
                line = line.strip("\n\r")
                tui._print(line[hor_index : hor_index + term_width - 7])
                tui.line_down(1)
                tui.move_cursor_right(3)
        tui.move_cursor((2, term_height - 1))
        control_str = "[ARROW-KEYS] Navigate\t[Q] Back"
        tui._print(control_str)
        tui._show()

    try:
        term_width, term_height = os.get_terminal_size()
        tui.clear_screen()
        get_logs()

        log_sched = BackgroundScheduler()
        log_sched.add_job(
            get_logs,
            "interval",
            seconds=15,
        )
        log_sched.start()

        vert_index = hor_index = 0
        render(vert_index, hor_index, term_width, term_height)
        prev_term_dims = (vert_index, hor_index, term_width, term_height)
        while True:
            b = tui.read_bytes(1)
            rerender = False
            if b in (b"r", b"R"):
                rerender = True
            elif b == b"\x1b":
                b = tui.read_bytes(2)
                if b == b"[A":  # Up Arrow
                    vert_index = max(0, vert_index - 1)
                elif b == b"[B":  # Down Arrow
                    vert_index += 1
                elif b == b"[D":  # Left Arrow
                    hor_index = max(0, hor_index - 5)
                elif b == b"[C":  # Right Arrow
                    hor_index += 5
                elif b == b"[1":  # Start of SHIFT + arrow keys
                    b = tui.read_bytes(3)
                    if b == b";2A":  # Up Arrow
                        vert_index = max(0, vert_index - 20)
                    elif b == b";2B":  # Down Arrow
                        vert_index += 20
                    elif b == b";2D":  # Left Arrow
                        hor_index = max(0, hor_index - 25)
                    elif b == b";2C":  # Right Arrow
                        hor_index += 25
            elif b == b"k":
                vert_index = max(0, vert_index - 1)
            elif b == b"j":
                vert_index += 1
            elif b == b"h":
                hor_index = max(0, hor_index - 5)
            elif b == b"l":
                hor_index += 5
            elif b == b"K":
                vert_index = max(0, vert_index - 20)
            elif b == b"J":
                vert_index += 20
            elif b == b"H":
                hor_index = max(0, hor_index - 25)
            elif b == b"L":
                hor_index += 25
            term_width, term_height = os.get_terminal_size()
            if rerender or (
                prev_term_dims != (vert_index, hor_index, term_width, term_height)
            ):
                tui.clear_screen()
                prev_term_dims = (vert_index, hor_index, term_width, term_height)
                render(vert_index, hor_index, term_width, term_height)
    except KeyboardInterrupt:
        ...
    finally:
        log_sched.shutdown()
        log_file.unlink(missing_ok=True)
        tui.clear_screen()
        tui.move_cursor((0, 0))
        tui._show()


# TODO(ayush): implement this
def _relaunch_modal(execution_data):
    return


def _abort_modal(execution_data):
    def render(term_width: int, term_height: int):
        # DISCLAIMER : MOST OF THE MAGIC NUMBERS HERE WERE THROUGH TRIAL AND ERROR
        tui.clear_screen()

        question = (
            f"Are you sure you want to abort {execution_data['display_name']} "
            f"({execution_data['workflow_name']}/{execution_data['workflow_version']})?"
        )
        lines = textwrap.wrap(question, width=term_width - 6)

        max_line_length = max(map(len, lines))

        x = (term_width - max_line_length) // 2
        y = (term_height - len(lines)) // 2

        tui.draw_box((x - 3, y - 2), len(lines) + 4, max_line_length + 4)

        for i, line in enumerate(lines):
            x = (term_width - len(line)) // 2
            tui.move_cursor((x, y + i))
            tui._print(f"{line}")

        ctrl_str = "[Y] Yes\t[N] No"
        tui.move_cursor(((term_width - len(ctrl_str)) // 2, y + len(lines) + 1))
        tui._print(ctrl_str)

    try:
        term_width, term_height = os.get_terminal_size()
        render(term_width, term_height)
        prev_term_dims = (term_width, term_height)
        while True:
            b = tui.read_bytes(1)
            if b in (b"y", b"Y"):
                headers = {"Authorization": f"Bearer {retrieve_or_login()}"}
                resp = post(
                    url=config.api.execution.abort,
                    headers=headers,
                    json={"execution_id": execution_data["id"]},
                )
                return
            elif b in (b"n", b"N"):
                return
            term_width, term_height = os.get_terminal_size()
            if prev_term_dims != (term_width, term_height):
                prev_term_dims = (term_width, term_height)
                render(term_width, term_height)
    except KeyboardInterrupt:
        ...
    finally:
        tui.clear_screen()
        tui.move_cursor((0, 0))
