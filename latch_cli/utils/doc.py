import textwrap
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field


def dedent_chunk(x: str) -> str:
    return textwrap.dedent(x).removeprefix("\n").removesuffix("\n")


@dataclass
class Doc:
    indent_level: int = field(default=0)

    lines: list[str] = field(default_factory=list, init=False)

    def print(self, *args, sep=" ", dedent=True) -> None:
        indent = self.indent_level * "  "

        strs = (str(x) for x in args)
        if dedent:
            strs = (dedent_chunk(x) for x in strs)

        lines = sep.join(strs).split("\n")

        for l in lines:
            self.lines.append(indent + l)

    @contextmanager
    def indent(self) -> Generator[None, None, None]:
        self.indent_level += 1
        try:
            yield
        finally:
            self.indent_level -= 1

    def render(self) -> str:
        return "\n".join(self.lines)
