import textwrap


# todo(maximsmol): use a stateful writer that keeps track of indent level
def reindent(x: str, level: int) -> str:
    if x[0] == "\n":
        x = x[1:]
    return textwrap.indent(textwrap.dedent(x), "    " * level)
