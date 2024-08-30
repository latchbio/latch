import os
import sys
import termios
import tty
from functools import wraps
from typing import Callable, List, Optional, Tuple, TypeVar

from typing_extensions import ParamSpec

from latch_cli.click_utils import AnsiCodes

from . import common

P = ParamSpec("P")
T = TypeVar("T")


def clear(k: int):
    """
    Clear `k` lines below the cursor, returning the cursor to the start of its original line
    """
    print(f"\x1b[2K\x1b[1E" * (k) + f"\x1b[{k}F")


def draw_box(
    ul_corner_pos: Tuple[int, int],
    height: int,
    width: int,
    color: Optional[str] = None,
):
    if height <= 0 or width <= 0:
        return
    move_cursor(ul_corner_pos)
    draw_horizontal_line(width, make_corner=True, color=color)
    draw_vertical_line(height, make_corner=True, color=color)
    draw_horizontal_line(width, left=True, make_corner=True, color=color)
    draw_vertical_line(height, up=True, make_corner=True, color=color)


def clear_screen():
    print("\x1b[2J")


def remove_cursor():
    print("\x1b[?25l")


def reveal_cursor():
    print("\x1b[?25h")


def move_cursor(pos: Tuple[int, int]):
    """
    Move the cursor to a given (x, y) coordinate
    """
    x, y = pos
    if x < 0 or y < 0:
        return
    print(f"\x1b[{y};{x}H")


def move_cursor_up(n: int):
    if n <= 0:
        return
    print(f"\x1b[{n}A")


def line_up(n: int):
    """Moves to the start of the destination line"""
    if n <= 0:
        return
    print(f"\x1b[{n}F")


def move_cursor_down(n: int):
    if n <= 0:
        return
    print(f"\x1b[{n}B")


def line_down(n: int):
    """Moves to the start of the destination line"""
    if n <= 0:
        return
    print(f"\x1b[{n}E")


def move_cursor_right(n: int):
    if n <= 0:
        return
    print(f"\x1b[{n}C")


def move_cursor_left(n: int):
    if n <= 0:
        return
    print(f"\x1b[{n}D")


def draw_vertical_line(
    height: int,
    up: bool = False,
    make_corner: bool = False,
    color: Optional[str] = None,
):
    """
    Draws a vertical line with given `height`, going upwards if `up` is True
    and downwards otherwise.
    """

    if height <= 0:
        return

    if color is not None:
        print(color)
    sep = "\x1b[1A" if up else "\x1b[1B"
    for i in range(height):
        if i == 0 and make_corner:
            corner = "\u2514" if up else "\u2510"
            print(f"{corner}\x1b[1D{sep}")
        else:
            print(f"\u2502\x1b[1D{sep}")
    if color is not None:
        print("\x1b[0m")


def draw_horizontal_line(
    width: int,
    left: bool = False,
    make_corner: bool = False,
    color: Optional[str] = None,
):
    """
    Draws a horizontal line with given `width`, going to the left if `left` is True
    and to the right otherwise.
    """

    if width <= 0:
        return

    if color is not None:
        print(color)
    sep = "\x1b[2D" if left else ""
    for i in range(width):
        if i == 0 and make_corner:
            corner = "\u2518" if left else "\u250c"
            print(f"{corner}{sep}")
        else:
            print(f"\u2500{sep}")
    if color is not None:
        print("\x1b[0m")


def raw_input(f: Callable[P, T]) -> Callable[P, T]:
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        old_settings = termios.tcgetattr(sys.stdin.fileno())
        tty.setraw(sys.stdin.fileno())

        try:
            return f(*args, **kwargs)
        finally:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)

    return wrapper


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


@raw_input
def select_tui(
    title: str, options: List[common.SelectOption[T]], clear_terminal: bool = True
) -> Optional[T]:
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

        print(title)
        line_down(2)

        num_lines_rendered = 4  # 4 "extra" lines for header + footer

        for i in range(start_index, start_index + max_per_page):
            if i >= len(options):
                break
            name = options[i]["display_name"]
            if i == curr_selected:
                color = AnsiCodes.color
                bold = AnsiCodes.bold
                reset = AnsiCodes.full_reset

                prefix = indent[:-2] + "> "

                print(f"{color}{bold}{prefix}{name}{reset}\x1b[1E")
            else:
                print(f"{indent}{name}\x1b[1E")
            num_lines_rendered += 1

        line_down(1)

        control_str = "[ARROW-KEYS] Navigate\t[ENTER] Select\t[Q] Quit"
        print(control_str)
        line_up(num_lines_rendered - 1)

        common.show()

        return num_lines_rendered

    curr_selected = 0
    start_index = 0
    _, term_height = os.get_terminal_size()
    remove_cursor()

    max_per_page = min(len(options), term_height - 4)

    if clear_terminal:
        clear_screen()
        move_cursor((0, 0))
    else:
        print("\n" * (max_per_page + 3))
        move_cursor_up(max_per_page + 4)

    num_lines_rendered = render(
        curr_selected,
        start_index=start_index,
        max_per_page=max_per_page,
    )

    try:
        while True:
            b = read_bytes(1)
            if b == b"\r":
                return options[curr_selected]["value"]
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
        reveal_cursor()
        common.show()
