import json
import os
import subprocess
import sys
import tempfile
import termios
import textwrap
import time
import tty
from typing import Callable, Dict, List, Optional, Tuple

from latch_cli.tinyrequests import get, post
from latch_cli.utils import current_workspace, retrieve_or_login


def buffered_print() -> Tuple[Callable, Callable]:
    buffer = []

    def __print(*args):
        for arg in args:
            buffer.append(arg)

    def __show():
        nonlocal buffer
        print("".join(buffer), flush=True, end="")
        buffer = []

    return __print, __show


# Allows for exactly one print per render, removing any weird flashing
# behavior and also speeding things up considerably
_print, _show = buffered_print()


def clear(k: int):
    """
    Clear `k` lines below the cursor, returning the cursor to its original position
    """
    _print(f"\x1b[2K\x1b[1E" * (k) + f"\x1b[{k}F")


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
    _print("\x1b[2J")


def remove_cursor():
    _print("\x1b[?25l")


def reveal_cursor():
    _print("\x1b[?25h")


def move_cursor(pos: Tuple[int, int]):
    """
    Move the cursor to a given (x, y) coordinate
    """
    x, y = pos
    if x < 0 or y < 0:
        return
    _print(f"\x1b[{y};{x}H")


def move_cursor_up(n: int):
    if n <= 0:
        return
    _print(f"\x1b[{n}A")


def line_up(n: int):
    """Moves to the start of the destination line"""
    if n <= 0:
        return
    _print(f"\x1b[{n}F")


def move_cursor_down(n: int):
    if n <= 0:
        return
    _print(f"\x1b[{n}B")


def line_down(n: int):
    """Moves to the start of the destination line"""
    if n <= 0:
        return
    _print(f"\x1b[{n}E")


def move_cursor_right(n: int):
    if n <= 0:
        return
    _print(f"\x1b[{n}C")


def move_cursor_left(n: int):
    if n <= 0:
        return
    _print(f"\x1b[{n}D")


def current_cursor_position() -> Tuple[int, int]:
    res = b""
    sys.stdout.write("\x1b[6n")
    sys.stdout.flush()
    while not res.endswith(b"R"):
        res += sys.stdin.buffer.read(1)
    y, x = res.strip(b"\x1b[R").split(b";")
    return int(x), int(y)


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
        _print(color)
    sep = "\x1b[1A" if up else "\x1b[1B"
    for i in range(height):
        if i == 0 and make_corner:
            corner = "\u2514" if up else "\u2510"
            _print(f"{corner}\x1b[1D{sep}")
        else:
            _print(f"\u2502\x1b[1D{sep}")
    if color is not None:
        _print("\x1b[0m")


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
        _print(color)
    sep = "\x1b[2D" if left else ""
    for i in range(width):
        if i == 0 and make_corner:
            corner = "\u2518" if left else "\u250c"
            _print(f"{corner}{sep}")
        else:
            _print(f"\u2500{sep}")
    if color is not None:
        _print("\x1b[0m")


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
