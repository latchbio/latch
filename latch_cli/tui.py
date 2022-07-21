import os
import sys
import termios
import tty
from typing import Dict, List


def _print(*args, **kwargs):
    print(*args, flush=True, end="", **kwargs)


def clear(k: int):
    """
    Clear `k` lines below the cursor, returning the cursor to its original position
    """
    _print(f"\x1b[2K\x1b[1E" * (k) + f"\x1b[{k}F")


def read_next_byte() -> bytes:
    b = sys.stdin.buffer.read(1)
    if b in (
        b"\x03",  # CTRL C
        b"\x04",  # CTRL D
        b"q",
        b"Q",
    ):
        raise KeyboardInterrupt
    return b


def read_bytes(num_bytes: int) -> bytes:
    if num_bytes < 0:
        raise ValueError(f"cannot read {num_bytes} bytes")
    result = b""
    for _ in range(num_bytes):
        result += read_next_byte()
    return result


def tui_select(title: str, options: List[str], clear_terminal: bool = False):
    """
    Renders a terminal UI that allows users to select one of the options listed in `options`

    Args:
        options: A list of names for each of the options.
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

        _print(title)
        _print("\x1b[2E")  # two new lines

        num_lines_rendered = 4  # 4 "extra" lines for header + footer

        for i in range(start_index, start_index + max_per_page):
            if i >= len(options):
                break
            name = options[i]
            if i == curr_selected:
                color = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                _print(f"{indent}{color}{bold}[{name}]{reset}\x1b[1E")
            else:
                _print(f"{indent}{name}\x1b[1E")
            num_lines_rendered += 1

        _print("\x1b[1E")

        control_str = "[ARROW-KEYS] Navigate\t[ENTER] Select\t[Q] Quit"
        _print(control_str)
        _print("\x1b[1E")

        _print(f"\x1b[{num_lines_rendered}F")

        return num_lines_rendered

    old_settings = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin.fileno())

    curr_selected = 0
    start_index = 0
    _, term_height = os.get_terminal_size()

    # Get cursor height
    res = b""
    sys.stdout.write("\x1b[6n")
    sys.stdout.flush()
    while not res.endswith(b"R"):
        res += sys.stdin.buffer.read(1)
    curs_height = int(res.strip(b"\x1b[").split(b";", 1)[0])

    max_per_page = term_height - curs_height - 4

    # Hide the cursor
    _print("\x1b[?25l")

    if clear_terminal:
        # This line
        #   (1) Clears the terminal window
        #   (2) Moves the cursor to the top left corner
        _print("\x1b[2J\x1b[H")

    num_lines_rendered = render(
        curr_selected,
        start_index=start_index,
        max_per_page=max_per_page,
    )

    try:
        while True:
            b = read_bytes(1)
            if b == b"\r":
                return options[curr_selected]
            elif b == b"\x1b":
                b = read_bytes(2)
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
            clear(num_lines_rendered)
            num_lines_rendered = render(
                curr_selected,
                start_index=start_index,
                max_per_page=max_per_page,
            )
    except KeyboardInterrupt:
        ...
    finally:
        clear(num_lines_rendered)
        # Unhide the cursor
        _print("\x1b[?25h")
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)


def tui_select_table(
    title: str,
    column_names: List[str],
    options: List[Dict[str, str]],
    clear_terminal: bool = False,
):
    if len(options) == 0:
        raise ValueError("No options given")
    elif len(column_names) == 0:
        raise ValueError("No column names specified")

    def render(
        curr_selected: int,
        term_width: int,
        start_index: int = 0,
        max_per_page: int = 10,
        indent: str = "    ",
        column_spacing: str = "  ",
    ) -> int:
        if curr_selected < 0 or curr_selected >= len(options):
            curr_selected = 0

        _print(title)
        _print("\x1b[2E")  # two new lines

        num_lines_rendered = 5  # 5 "extra" lines for header + footer

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

            if len(row_str) > term_width - 2:
                row_str = row_str[: term_width - 5] + "... "

            if i == curr_selected:
                color = "\x1b[38;5;40m"
                bold = "\x1b[1m"
                reset = "\x1b[0m"
                row_str = f"{color}{bold}{row_str}{reset}"

            _print(f"{row_str}\x1b[1E")
            num_lines_rendered += 1

        _print("\x1b[1E")

        control_str = "[ARROW-KEYS] Navigate\t[ENTER] Select\t[Q] Quit"
        _print(control_str)
        _print("\x1b[1E")

        _print(f"\x1b[{num_lines_rendered}F")

        return num_lines_rendered

    old_settings = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin.fileno())

    curr_selected = 0
    start_index = 0
    term_width, term_height = os.get_terminal_size()

    # Get cursor height
    res = b""
    sys.stdout.write("\x1b[6n")
    sys.stdout.flush()
    while not res.endswith(b"R"):
        res += sys.stdin.buffer.read(1)
    curs_height = int(res.strip(b"\x1b[").split(b";", 1)[0])

    max_per_page = term_height - curs_height - 4

    # Hide the cursor
    _print("\x1b[?25l")

    if clear_terminal:
        # This line
        #   (1) Clears the terminal window
        #   (2) Moves the cursor to the top left corner
        _print("\x1b[2J\x1b[H")

    num_lines_rendered = render(
        curr_selected,
        start_index=start_index,
        max_per_page=max_per_page,
        term_width=term_width,
    )

    try:
        while True:
            b = read_bytes(1)
            if b == b"\r":
                return options[curr_selected]
            elif b == b"\x1b":
                b = read_bytes(2)
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
            clear(num_lines_rendered)
            num_lines_rendered = render(
                curr_selected,
                start_index=start_index,
                max_per_page=max_per_page,
                term_width=term_width,
            )
    except KeyboardInterrupt:
        ...
    finally:
        clear(num_lines_rendered)
        # Unhide the cursor
        _print("\x1b[?25h")
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)


if __name__ == "__main__":

    title = "Sample Selection Screen"

    options = [f"Option {i}" for i in range(100)]

    selected = tui_select(title, options)

    if selected:
        print(selected)
