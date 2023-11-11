from enum import Enum
from typing import Any, Dict, IO, List, Optional, Sequence, Tuple, Type

from click import Choice, Command, Context, Group, HelpFormatter, Parameter, echo, style
from click._compat import get_text_stderr

from enum import Enum
from typing import Any, Dict, IO, List, Optional, Sequence, Tuple, Type

from click import Choice, Command, Context, Group, HelpFormatter, Parameter, echo, style
from click._compat import get_text_stderr


def _levenshtein(str1: str, str2: str, swap: int = 0, substitution: int = 2, insertion: int = 1, deletion: int = 4) -> int:
    """Computes the levenshtein distance between two strings"""

    row0 = [0 for _ in range(len(str2)+1)]
    row1 = [i * insertion for i in range(len(str2)+1)]
    row2 = [0 for _ in range(len(str2)+1)]

    for idx1, char1 in enumerate(str1):
        row2[0] = (idx1 + 1) * deletion
        for idx2, char2 in enumerate(str2):
            row2[idx2+1] = row1[idx2]

            if str1[idx1] != str2[idx2]:
                row2[idx2+1] += substitution

            if idx1 > 0 and idx2 > 0 and str1[idx1-1] == str2[idx2] and str1[idx1] == str2[idx2-1] and row2[idx2+1] > row0[idx2-1] + swap:
                row2[idx2+1] = row0[idx2-1] + swap

            if row2[idx2+1] > row1[idx2+1] + deletion:
                row2[idx2+1] = row1[idx2+1] + deletion

            if row2[idx2+1] > row2[idx2] + insertion:
                row2[idx2+1] = row2[idx2] + insertion

        dummy = row0
        row0 = row1
        row1 = row2
        row2 = dummy

    return row1[len(str2)]


def _find_smallest_similarity_distance(
    target: str,
    options: List[str],
    distances: Dict[str, int],
    unknown_similarity_floor: int = 7,
    unknown_substring_length: int = 5,
) -> Optional[int]:
    """Finds the the smallest similarity distance between the target and options, returning `None` if
    not exactly one (i.e. none or more than one) was found.
    """
    best_distance: Optional[int] = None
    num_best: int = 0
    for name in options:
        assert name != target, f"Bug: {name} should not match the target"
        distance: int
        if name.startswith(target):
            distance = 0
        elif unknown_substring_length <= len(target) and target in name:
            distance = 0
        else:
            distance = _levenshtein(str1=target, str2=name)
        distances[name] = distance
        if best_distance is None or distance < best_distance:
            best_distance = distance
            num_best = 1
        elif distance == best_distance:
            num_best += 1

    if best_distance is None:
        return None
    elif 0 == best_distance and 1 < num_best and num_best == len(options):
        return None
    else:
        return best_distance


def _find_similar(
    target: str,
    options: List[str],
    unknown_similarity_floor: int = 7,
    unknown_substring_length: int = 5) -> List[str]:
    """When a command does not match any known command, searches for similar commands, using the same method as GIT"""
    distances: Dict[str, int] = {}
    best_distance: Optional[int] = _find_smallest_similarity_distance(
        target=target,
        options=options,
        distances=distances,
        unknown_similarity_floor=unknown_similarity_floor,
        unknown_substring_length=unknown_substring_length,
    )
    if best_distance is None or unknown_similarity_floor <= best_distance:
        return []
    else:
        return [
            name
            for name in options
            if distances[name] == best_distance
        ]


class EnumChoice(Choice):
    def __init__(self, choices: Type[Enum], case_sensitive: bool = True):
        self.enum = choices
        return super().__init__(choices._member_names_, case_sensitive)

    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> Any:
        ret = super().convert(value, param, ctx)
        return self.enum(ret)

    # todo(ayush): override `shell_complete` once we support it


class ColoredHelpFormatter(HelpFormatter):
    def write_usage(
        self, prog: str, args: str = "", prefix: Optional[str] = None
    ) -> None:
        if prefix is None:
            prefix = "Usage: "

        prefix = style(prefix, fg="bright_blue")
        return super().write_usage(prog, args, prefix)

    def write_heading(self, heading: str):
        return super().write_heading(style(heading, fg="bright_blue"))

    def write_dl(
        self, rows: Sequence[Tuple[str, str]], col_max: int = 30, col_spacing: int = 2
    ) -> None:
        return super().write_dl(
            [(style(x[0], fg="bright_green", italic=True), x[1]) for x in rows],
            col_max,
            col_spacing,
        )


class LatchCommand(Command):
    def format_epilog(self, ctx: Context, formatter: HelpFormatter) -> None:
        formatter.write_paragraph()
        formatter.write_text(
            style("See " + style("https://docs.latch.bio/", underline=True), dim=True)
            + style(" for manuals, tutorials, examples, and an API reference", dim=True)
        )



----
class EnumChoice(Choice):
    def __init__(self, choices: Type[Enum], case_sensitive: bool = True):
        self.enum = choices
        return super().__init__(choices._member_names_, case_sensitive)

    def convert(
        self, value: Any, param: Optional[Parameter], ctx: Optional[Context]
    ) -> Any:
        ret = super().convert(value, param, ctx)
        return self.enum(ret)

    # todo(ayush): override `shell_complete` once we support it


class ColoredHelpFormatter(HelpFormatter):
    def write_usage(
        self, prog: str, args: str = "", prefix: Optional[str] = None
    ) -> None:
        if prefix is None:
            prefix = "Usage: "

        prefix = style(prefix, fg="bright_blue")
        return super().write_usage(prog, args, prefix)

    def write_heading(self, heading: str):
        return super().write_heading(style(heading, fg="bright_blue"))

    def write_dl(
        self, rows: Sequence[Tuple[str, str]], col_max: int = 30, col_spacing: int = 2
    ) -> None:
        return super().write_dl(
            [(style(x[0], fg="bright_green", italic=True), x[1]) for x in rows],
            col_max,
            col_spacing,
        )


class LatchCommand(Command):
    def format_epilog(self, ctx: Context, formatter: HelpFormatter) -> None:
        formatter.write_paragraph()
        formatter.write_text(
            style("See " + style("https://docs.latch.bio/", underline=True), dim=True)
            + style(" for manuals, tutorials, examples, and an API reference", dim=True)
        )


class LatchGroup(LatchCommand, Group):
    def get_command(self, ctx, cmd_name):
        rv = Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
        
        # check for exact matches
        matches = [
            x for x in self.list_commands(ctx) 
            if x == cmd_name
        ]
        if len(matches) > 1:
            ctx.fail(f"Too many matches: {', '.join(sorted(matches))}")
        elif len(matches) == 1:
            return Group.get_command(self, ctx, matches[0])
        
        # check for similarity the same way GIT does
        matches = _find_similar(cmd_name, self.list_commands(ctx))
        if len(matches) == 0:
            return None
        word = "this" if len(matches) == 1 else "one of these"
        option_sep = "\n        "
        ctx.fail(
            f"No such command '{cmd_name}'\nDid you mean {word}?{option_sep}" + option_sep.join(matches)
        )

    def resolve_command(self, ctx, args):
        # always return the full command name
        _, cmd, args = super().resolve_command(ctx, args)
        return cmd.name, cmd, args


def colored_exception_show(self, file: Optional[IO] = None) -> None:
    if file is None:
        file = get_text_stderr()

    echo(
        style(f"Error: {style(self.format_message(), bold=True)}", fg="red"), file=file
    )


def colored_usage_error_show(self, file: Optional[IO] = None) -> None:
    if file is None:
        file = get_text_stderr()
    color = None
    hint = ""
    if self.ctx is not None and self.ctx.command.get_help_option(self.ctx) is not None:
        hint = (
            "Try "
            + style(
                f"'{self.ctx.command_path} {self.ctx.help_option_names[0]}'",
                bold=True,
            )
            + " for help."
        )
        hint = f"{hint}\n"
    if self.ctx is not None:
        color = self.ctx.color
        echo(f"{self.ctx.get_usage()}\n{hint}", file=file, color=color)
    echo(
        style(f"Error: {style(self.format_message(), bold=True)}", fg="red"),
        file=file,
        color=color,
    )


def patch():
    import click

    click.Context.formatter_class = ColoredHelpFormatter

    old_group = click.group
    click.group = lambda *args, **kwargs: old_group(*args, **kwargs, cls=LatchGroup)
    Group.command_class = LatchCommand

    click.ClickException.show = colored_exception_show
    click.UsageError.show = colored_usage_error_show


def bold(s: str) -> str:
    return f"{AnsiCodes.bold}{s}{AnsiCodes.reset}"


class AnsiCodes:
    bold = "\x1b[1m"
    reset = "\x1b[22m"

    underline = "\x1b[4m"
    no_underline = "\x1b[24m"

    # todo(maximsmol): use in supported terminals?
    url_href = "\x1b]8;;"
    url_name = "\x1b\\"
    url_end = "\x1b]8;;\x1b\\"
