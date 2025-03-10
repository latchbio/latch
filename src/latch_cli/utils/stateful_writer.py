from contextlib import contextmanager


class StatefulWriter:
    def __init__(self, indent: int = 4):
        self._indent = " " * indent

        self._buf = []
        self._cur = ""

    @contextmanager
    def indent(self):
        self._cur += self._indent
        yield
        self._cur = self._cur.removesuffix(self._indent)

    def clear(self):
        self._buf = []
        self._cur = ""

    def write(self, s: str, *, nl: bool = True):
        self._buf.append(self._indent)
        self._buf.append(s)

        if nl:
            self._buf.append("\n")

    def get(self):
        return "".join(self._buf)
