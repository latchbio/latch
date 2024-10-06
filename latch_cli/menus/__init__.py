import sys

if sys.platform == "win32":
    from .win32 import raw_input, select_tui
else:
    from .vt100 import raw_input, select_tui

from .common import *
