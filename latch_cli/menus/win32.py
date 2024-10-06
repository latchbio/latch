import msvcrt
import os
import sys
from ctypes import byref, c_char, c_long, c_ulong, pointer, windll
from ctypes.wintypes import DWORD, HANDLE
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Callable, List, Optional, Tuple, TypeVar

from typing_extensions import ParamSpec

from latch_cli.click_utils import AnsiCodes

from . import common
from .win32_types import (
    CONSOLE_SCREEN_BUFFER_INFO,
    COORD,
    INPUT_RECORD,
    KEY_EVENT_RECORD,
    MOUSE_EVENT_RECORD,
    STD_INPUT_HANDLE,
    STD_OUTPUT_HANDLE,
    EventTypes,
)

P = ParamSpec("P")
T = TypeVar("T")


hconsole = HANDLE(windll.kernel32.GetStdHandle(STD_OUTPUT_HANDLE))


# convenience dataclasses for readability
@dataclass
class Pair:
    x: int
    y: int


@dataclass
class ConsoleScreenBufferInfo:
    raw: CONSOLE_SCREEN_BUFFER_INFO
    buffer_size: Pair
    cursor_position: Pair
    attributes: int
    window_top_left: Pair
    window_bottom_right: Pair
    max_window_size: Pair

    def window_dimensions(self) -> Pair:
        return Pair(
            x=self.window_bottom_right.x - self.window_top_left.x,
            y=self.window_bottom_right.y - self.window_top_left.y,
        )


def get_win32_screen_buffer_info() -> ConsoleScreenBufferInfo:
    """
    https://github.com/prompt-toolkit/python-prompt-toolkit/blob/669541123c9a72da1fda662cbd0a18ffe9e6d113/src/prompt_toolkit/output/win32.py#L185
    """

    sbinfo = CONSOLE_SCREEN_BUFFER_INFO()

    success = windll.kernel32.GetConsoleScreenBufferInfo(hconsole, byref(sbinfo))

    if success:
        return ConsoleScreenBufferInfo(
            raw=sbinfo,
            buffer_size=Pair(
                x=sbinfo.dwSize.X,
                y=sbinfo.dwSize.Y,
            ),
            cursor_position=Pair(
                x=sbinfo.dwCursorPosition.X,
                y=sbinfo.dwCursorPosition.Y,
            ),
            attributes=sbinfo.wAttributes,
            window_top_left=Pair(
                x=sbinfo.srWindow.Left,
                y=sbinfo.srWindow.Top,
            ),
            window_bottom_right=Pair(
                x=sbinfo.srWindow.Right,
                y=sbinfo.srWindow.Bottom,
            ),
            max_window_size=Pair(
                x=sbinfo.dwMaximumWindowSize.X,
                y=sbinfo.dwMaximumWindowSize.Y,
            ),
        )
    else:
        raise RuntimeError("No windows console found")


def _erase(start: COORD, length: int) -> None:
    """
    https://github.com/prompt-toolkit/python-prompt-toolkit/blob/669541123c9a72da1fda662cbd0a18ffe9e6d113/src/prompt_toolkit/output/win32.py#L255
    """
    chars_written = c_ulong()

    windll.kernel32.FillConsoleOutputCharacterA(
        hconsole, c_char(b" "), DWORD(length), start, byref(chars_written)
    )

    # Reset attributes.
    sbinfo = get_win32_screen_buffer_info()
    windll.kernel32.FillConsoleOutputAttribute(
        hconsole, sbinfo.attributes, length, start, byref(chars_written)
    )


def clear(k: int):
    """
    Clear `k` lines below the cursor, returning the cursor to the start of its original line
    """
    sbinfo = get_win32_screen_buffer_info()

    length = sbinfo.window_dimensions().x * k
    _erase(sbinfo.raw.dwCursorPosition, length)

    # does the cursor move at all here? do we need to move back to the original line?


def clear_screen():
    sbinfo = get_win32_screen_buffer_info()
    dims = sbinfo.window_dimensions()

    start = COORD(X=sbinfo.window_top_left.x, Y=sbinfo.window_top_left.y)
    length = dims.x * dims.y

    _erase(start, length)


def remove_cursor():
    pass  # not supported in windows afaict


def reveal_cursor():
    pass  # not supported in windows afaict


def move_cursor(x: int, y: int):
    """
    Move the cursor to a given (x, y) coordinate
    """
    if x < 0 or y < 0:
        return

    windll.kernel32.SetConsoleCursorPosition(hconsole, COORD(X=x, Y=y))


def move_cursor_up(n: int):
    if n <= 0:
        return

    sbinfo = get_win32_screen_buffer_info()

    x = sbinfo.cursor_position.x
    y = sbinfo.cursor_position.y - n
    move_cursor(x, y)


def line_up(n: int):
    """Moves to the start of the destination line"""
    sbinfo = get_win32_screen_buffer_info()

    x = 0
    y = sbinfo.cursor_position.y - n
    move_cursor(x, y)


def line_down(n: int):
    """Moves to the start of the destination line"""

    line_up(-n)


def raw_input(f: Callable[P, T]) -> Callable[P, T]:
    # ayush: got most of this from
    # https://github.com/prompt-toolkit/python-prompt-toolkit/blob/669541123c9a72da1fda662cbd0a18ffe9e6d113/src/prompt_toolkit/input/win32.py
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:

        original_mode = DWORD()
        handle = HANDLE(windll.kernel32.GetStdHandle(STD_INPUT_HANDLE))
        windll.kernel32.GetConsoleMode(handle, pointer(original_mode))

        enable_echo_input = 0x0004
        enable_line_input = 0x0002
        enable_processed_input = 0x0001

        windll.kernel32.SetConsoleMode(
            handle,
            original_mode.value
            & ~(enable_echo_input | enable_line_input | enable_processed_input),
        )

        try:
            return f(*args, **kwargs)
        finally:
            windll.kernel32.SetConsoleMode(handle, original_mode)

    return wrapper


class Special(Enum):
    left = "left"
    right = "right"
    up = "up"
    down = "down"
    enter = "enter"
    ctrlc = "ctrlc"
    ctrld = "ctrld"


@dataclass
class KeyInput:
    special: Optional[Special]
    value: str


# ignoring most things except for arrow keys / enter
def get_key_input() -> Optional[KeyInput]:
    # ayush:
    # https://github.com/prompt-toolkit/python-prompt-toolkit/blob/669541123c9a72da1fda662cbd0a18ffe9e6d113/src/prompt_toolkit/input/win32.py#L127
    handle: HANDLE
    if sys.stdin.isatty():
        handle = HANDLE(windll.kernel32.GetStdHandle(STD_INPUT_HANDLE))
    else:
        _fdcon = os.open("CONIN$", os.O_RDWR | os.O_BINARY)
        handle = HANDLE(msvcrt.get_osfhandle(_fdcon))

    max_count = 2048  # Max events to read at the same time.

    read = DWORD(0)
    arrtype = INPUT_RECORD * max_count
    input_records = arrtype()

    windll.kernel32.ReadConsoleInputW(
        handle, pointer(input_records), max_count, pointer(read)
    )

    for i in range(read.value):
        ir = input_records[i]

        if ir.EventType not in EventTypes:
            continue

        ev = getattr(ir.Event, EventTypes[ir.EventType])

        if not (isinstance(ev, KEY_EVENT_RECORD) and ev.KeyDown):
            continue

        u_char = ev.uChar.UnicodeChar

        special: Optional[Special] = None
        if u_char == "\x00":  # special keys, e.g. arrow keys
            if ev.VirtualKeyCode == 37:
                special = Special.left
            elif ev.VirtualKeyCode == 38:
                special = Special.up
            elif ev.VirtualKeyCode == 39:
                special = Special.right
            elif ev.VirtualKeyCode == 40:
                special = Special.down
        elif u_char == "\x0d":
            special = Special.enter
        elif u_char == "\x03":
            special = Special.ctrlc
        elif u_char == "\x04":
            special = Special.ctrld

        return KeyInput(special, u_char)


def set_attrs(color: int = 0xB, bold: bool = True) -> int:
    common.show()

    sbinfo = get_win32_screen_buffer_info()
    attrs = sbinfo.attributes

    attrs = attrs & ~0xF
    attrs |= color

    if bold:
        attrs |= 1 << 8  # bold
    else:
        attrs &= ~(1 << 8)

    windll.kernel32.SetConsoleTextAttribute(hconsole, attrs)

    return sbinfo.attributes


def reset_attrs(attrs: int):
    windll.kernel32.SetConsoleTextAttribute(hconsole, attrs)


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
        line_down(1)

        num_lines_rendered = 5  # 4 "extra" lines for header + footer

        for i in range(start_index, start_index + max_per_page):
            if i >= len(options):
                break

            name = options[i]["display_name"]

            if i == curr_selected:
                prefix = indent[:-2] + "> "

                old = set_attrs()
                print(f"{prefix}{name}")
                reset_attrs(old)
            else:
                print(f"{indent}{name}")

            num_lines_rendered += 1

            line_down(1)

        control_str = "[ARROW-KEYS] Navigate\t[ENTER] Select\t[Q] Quit"
        print(control_str)
        line_up(num_lines_rendered - 1)

        return num_lines_rendered

    curr_selected = 0
    start_index = 0
    _, term_height = os.get_terminal_size()
    remove_cursor()

    max_per_page = min(len(options), term_height - 4)

    if clear_terminal:
        clear_screen()
        move_cursor(0, 0)
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
            k = get_key_input()
            if k is None:
                continue

            if k.special == Special.enter:
                return options[curr_selected]["value"]
            elif k.special == Special.up:  # Up Arrow
                curr_selected = max(curr_selected - 1, 0)
                if curr_selected - start_index < max_per_page // 2 and start_index > 0:
                    start_index -= 1
            elif k.special == Special.down:  # Down Arrow
                curr_selected = min(curr_selected + 1, len(options) - 1)
                if (
                    curr_selected - start_index > max_per_page // 2
                    and start_index < len(options) - max_per_page
                ):
                    start_index += 1
            elif k.special in {Special.ctrlc, Special.ctrld} or k.value in {"q", "Q"}:
                return
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
