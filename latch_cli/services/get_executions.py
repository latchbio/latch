import os
import sys
import termios
import textwrap
import tty
from typing import Dict, List

import latch_cli.tui as tui
from latch_cli.config.latch import LatchConfig
from latch_cli.tinyrequests import post
from latch_cli.utils import current_workspace, retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def get_executions():
    token = retrieve_or_login()
    context = current_workspace()
    headers = {"Authorization": f"Bearer {token}"}

    resp = post(
        url=endpoints["get-executions"],
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

    all_executions_tui(
        title="Executions",
        column_names=display_columns,
        options=options,
        clear_terminal=True,
    )


def all_executions_tui(
    title: str,
    column_names: List[str],
    options: List[Dict[str, str]],
):
    if len(options) == 0:
        raise ValueError("No options given")
    elif len(column_names) == 0:
        raise ValueError("No column names specified")

    def render(
        curr_selected: int,
        term_width: int,
        term_height: int,
        indent: str = "    ",
        column_spacing: str = "  ",
    ) -> int:
        if curr_selected < 0 or curr_selected >= len(options):
            return

        max_per_page = term_height - 4

        start_index = max(0, curr_selected - max_per_page // 2)

        tui._print(title)
        tui.line_down(2)

        # 5 "extra" lines for header + footer
        num_lines_rendered = 5

        lengths = {col: len(col) for col in column_names}
        for i in range(len(options)):
            values = options[i]
            for col in column_names:
                lengths[col] = max(lengths[col], len(values[col]))

        for i in range(start_index, start_index + max_per_page):
            if i >= len(options):
                break
            values = options[i]
            row_str = indent
            for col in column_names:
                item = values[col]
                row_str = row_str + f"{item: <{lengths[col]}}" + column_spacing

            if len(row_str) > term_width - 4:
                row_str = row_str[: term_width - 6] + "..."

            if i == curr_selected:
                green = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                row_str = f"{green}{bold}{row_str}{reset}"

            tui._print(row_str)
            tui.line_down(1)
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
    term_width, term_height = os.get_terminal_size()

    tui.remove_cursor()
    tui.move_cursor((0, 0))

    num_lines_rendered = render(
        curr_selected,
        term_height=term_height,
        term_width=term_width,
    )

    try:
        while True:
            prev = (
                term_width,
                term_height,
                curr_selected,
            )
            rerender = False
            b = tui.read_bytes(1)
            if b == b"\r":
                selected_execution_data = options[curr_selected]
                resp = post(
                    endpoints["get-workflow-graph"],
                    headers={"Authorization": f"Bearer {retrieve_or_login()}"},
                    json={"workflow_id": selected_execution_data["workflow_id"]},
                )
                execution_dashboard_tui(selected_execution_data, resp.json())
                rerender = True
            elif b == b"\x1b":
                b = tui.read_bytes(2)
                if b == b"[A":  # Up Arrow
                    curr_selected = max(curr_selected - 1, 0)
                elif b == b"[B":  # Down Arrow
                    curr_selected = min(curr_selected + 1, len(options) - 1)
                else:
                    continue
            elif b == b"k":
                curr_selected = max(curr_selected - 1, 0)
            elif b == b"j":
                curr_selected = min(curr_selected + 1, len(options) - 1)
            term_width, term_height = os.get_terminal_size()
            if rerender or ((term_width, term_height, curr_selected) != prev):
                tui.clear(num_lines_rendered)
                num_lines_rendered = render(
                    curr_selected,
                    term_height=term_height,
                    term_width=term_width,
                )
    except KeyboardInterrupt:
        ...
    finally:
        tui.clear(num_lines_rendered)
        tui.reveal_cursor()
        tui._show()
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)


def execution_dashboard_tui(execution_data: Dict[str, str], workflow_graph: Dict):
    fixed_workflow_graph = list(workflow_graph.items())

    def render(curr_selected: int, term_width: int, term_height: int):
        tui.draw_box((2, 2), term_height - 4, term_width - 4)

        tui.move_cursor((2, term_height - 1))
        instructions = [
            "[ARROW-KEYS] Navigate",
            "[ENTER] View Task Logs",
            "[R] Relaunch",
        ]
        if execution_data["status"] == "RUNNING":
            instructions.append("[A] Abort")
        instructions.append("[Q] Quit")
        tui._print("\t".join(instructions))

        tui.move_cursor((4, 3))
        for i, (id, task) in enumerate(fixed_workflow_graph):
            if i == curr_selected:
                green = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                tui._print(f"{green}{bold}{task['name']}{reset}")
            else:
                tui._print(task["name"])
            tui.line_down(1)
            tui.move_cursor_right(3)
        tui._show()

    tui.clear_screen()
    tui.remove_cursor()

    try:
        term_width, term_height = os.get_terminal_size()
        curr_selected = 0

        render(curr_selected, term_width, term_height)
        prev = (curr_selected, term_width, term_height)
        while True:
            b = tui.read_bytes(1)
            rerender = False
            if b == b"\r":
                log_window(execution_data, fixed_workflow_graph, curr_selected)
                rerender = True
            elif b in (b"r", b"R"):
                relaunch_modal(execution_data)
                rerender = True
            elif b in (b"a", b"A"):
                abort_modal(execution_data)
                rerender = True
            elif b == b"\x1b":
                b = tui.read_bytes(2)
                if b == b"[A":  # Up Arrow
                    curr_selected = max(curr_selected - 1, 0)
                elif b == b"[B":  # Down Arrow
                    curr_selected = min(curr_selected + 1, len(workflow_graph) - 1)
                else:
                    continue
            elif b in (b"j", b"J"):
                curr_selected = min(curr_selected + 1, len(workflow_graph) - 1)
            elif b in (b"k", b"K"):
                curr_selected = max(curr_selected - 1, 0)
            term_width, term_height = os.get_terminal_size()
            if rerender or (prev != (curr_selected, term_width, term_height)):
                tui.clear_screen()
                prev = (curr_selected, term_width, term_height)
                render(curr_selected, term_width, term_height)
    except KeyboardInterrupt:
        ...
    finally:
        tui.clear_screen()
        tui.reveal_cursor()
        tui.move_cursor((0, 0))
        tui._show()


def log_window(execution_data, fixed_workflow_graph: list, selected: int):
    id, selected_task = fixed_workflow_graph[selected]

    def get_logs():
        resp = post(
            "https://nucleus.latch.bio/sdk/get-logs-for-node",
            headers={"Authorization": f"Bearer {retrieve_or_login()}"},
            json={
                "exec_id": execution_data["id"],
                "node_id": selected_task["node_id"],
                "account_id": current_workspace(),
            },
        )
        resp.raise_for_status()
        return resp.json()["message"]

    def render(term_width, term_height):
        tui.draw_box((2, 2), term_height - 4, term_width - 4)

        message = get_logs()
        with open("cache.txt", "w") as f:
            f.write(message)

        tui.move_cursor((4, 3))
        for line in message.split("\n"):
            tui._print(line)
            tui.line_down(1)
            tui.move_cursor_left(4)
        tui._show()

    try:
        term_width, term_height = os.get_terminal_size()
        tui.clear_screen()
        render(term_width, term_height)
        prev_term_dims = (term_width, term_height)
        while True:
            _ = tui.read_bytes(1)
            term_width, term_height = os.get_terminal_size()
            if prev_term_dims != (term_width, term_height):
                tui.clear_screen()
                prev_term_dims = (term_width, term_height)
                render(term_width, term_height)
    except KeyboardInterrupt:
        ...
    finally:
        tui.clear_screen()
        tui.move_cursor((0, 0))
        tui._show()


def relaunch_modal(execution_data):
    # WIP

    # import google.protobuf.json_format as gpjson
    # from flyteidl.core.literals_pb2 import LiteralMap as _LiteralMap
    # from flyteidl.core.types_pb2 import LiteralType as _LiteralType
    # from flytekit.core.context_manager import FlyteContext, FlyteContextManager
    # from flytekit.core.type_engine import TypeEngine
    # from flytekit.models.literals import LiteralMap
    # from flytekit.models.types import LiteralType

    # from latch_cli.services.get_params import (
    #     _get_code_literal,
    #     _guess_python_type,
    #     _guess_python_val,
    # )

    # token = retrieve_or_login()
    # headers = {"Authorization": f"Bearer {token}"}

    # response = post(
    #     "https://nucleus.latch.bio/sdk/get-execution-inputs",
    #     headers=headers,
    #     json={"execution_id": execution_data["id"]},
    # )

    # data = response.json()
    # url = data["url"]
    # input_schema = json.loads(data["input_schema"])
    # variables = input_schema["variables"]

    # type_map = {}

    # for var in variables.values():
    #     try:
    #         description_json = json.loads(var["description"])
    #         param_name = description_json["name"]
    #     except (json.decoder.JSONDecodeError, KeyError) as e:
    #         ...

    #     literal_type_json = var["type"]
    #     literal_type = gpjson.ParseDict(literal_type_json, _LiteralType())

    #     type_map[param_name] = LiteralType.from_flyte_idl(literal_type)
    # relaunch_bytes = get(url).content
    # relaunch_proto = _LiteralMap()
    # relaunch_proto.ParseFromString(relaunch_bytes)

    # literal_map = LiteralMap.from_flyte_idl(relaunch_proto)
    # literal_names = list(literal_map.literals.keys()).copy()
    # for param_name in literal_names:
    #     if param_name not in type_map:
    #         literal_map.literals.pop(param_name)

    # param_map = TypeEngine.literal_map_to_kwargs(
    #     FlyteContextManager.current_context(),
    #     literal_map,
    #     type_map,
    # )

    # with tempfile.NamedTemporaryFile("w") as ntf:
    #     # ntf.write(str(literal_map.literals["param_1"].__dict__))
    #     # ntf.write(str(literal_map))
    #     # ntf.write("\n" * 10)
    #     # ntf.write(str(type_map))
    #     ntf.write(str(param_map))
    #     ntf.seek(0)  # lol

    #     subprocess.call(["vim", ntf.name])

    # remove_cursor()
    return


def abort_modal(execution_data):
    def render(term_width: int, term_height: int):
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
                    url="https://nucleus.latch.bio/sdk/abort-execution",
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


if __name__ == "__main__":
    old_settings = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin.fileno())
    tui.clear_screen()
    x, y = tui.current_cursor_position()
    tui._print(str(x), str(y))
    tui._show()
    termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)
