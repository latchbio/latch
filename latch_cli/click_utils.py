from enum import Enum
from typing import IO, Any, Optional, Sequence, Tuple, Type

from click import Choice, Command, Context, Group, HelpFormatter, Parameter, echo, style
from click._compat import get_text_stderr


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
    ...


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


class AnsiCodes:
    bold = "\x1b[1m"
    reset = "\x1b[22m"

    underline = "\x1b[4m"
    no_underline = "\x1b[24m"

    # todo(maximsmol): use in supported terminals?
    url_href = "\x1b]8;;"
    url_name = "\x1b\\"
    url_end = "\x1b]8;;\x1b\\"
