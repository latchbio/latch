import ast
import os
import traceback
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from textwrap import dedent
from typing import Literal, Optional

import click

from latch.resources import tasks


@dataclass
class FlyteObject:
    type: Literal["task", "workflow"]
    name: str
    dockerfile: Optional[Path] = None


task_decorators = set(filter(lambda x: x.endswith("task"), tasks.__dict__.keys()))


class Visitor(ast.NodeVisitor):
    def __init__(self, file: Path):
        self.file = file
        self.module_name = str(file.with_suffix("")).replace(os.pathsep, ".")
        self.flyte_objects: list[FlyteObject] = []

    # todo(ayush): skip defs that arent run on import
    def visit_FunctionDef(self, node: ast.FunctionDef):  # noqa: N802
        if len(node.decorator_list) == 0:
            return self.generic_visit(node)

        name = f"{self.module_name}.{node.name}"

        # todo(ayush): |
        # 1. support ast.Attribute (@latch.tasks.small_task)
        # 2. normalize to fqn before checking whether or not its a task decorator
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name):
                if decorator.id == "workflow":
                    self.flyte_objects.append(FlyteObject("workflow", name))
                elif decorator.id in task_decorators:
                    self.flyte_objects.append(FlyteObject("task", name))

            elif isinstance(decorator, ast.Call):
                func = decorator.func
                assert isinstance(func, ast.Name)

                if func.id not in task_decorators and func.id != "workflow":
                    continue

                if func.id == "workflow":
                    self.flyte_objects.append(FlyteObject("workflow", name))
                    continue

                # note(ayush): this only works if `dockerfile` is a keyword arg - if someone
                # is insane enough to pass in the 14 other arguments first then have `dockerfile`
                # as a positional arg i will fix it
                dockerfile: Optional[Path] = None
                for kw in decorator.keywords:
                    if kw.arg != "dockerfile":
                        continue

                    try:
                        provided: str = ast.literal_eval(kw.value)
                    except ValueError as e:
                        click.secho(
                            dedent(f"""\
                                There was an issue parsing the `dockerfile` argument for task `{name}` in {self.file}.
                                Note that values passed to `dockerfile` must be string literals.
                            """),
                            fg="red",
                        )

                        raise click.exceptions.Exit(1) from e

                    dockerfile = self.file.parent / provided

                    if not dockerfile.exists():
                        click.secho(
                            f"""\
                            The `dockerfile` value (provided {provided}, resolved to {dockerfile}) for task `{name}` in {self.file} does not exist.
                            Note that relative paths are resolved with respect to the parent directory of the file.\
                            """,
                            fg="red",
                        )

                        raise click.exceptions.Exit(1)

                self.flyte_objects.append(FlyteObject("task", name, dockerfile))

        return self.generic_visit(node)


def get_flyte_objects(p: Path) -> list[FlyteObject]:
    res: list[FlyteObject] = []
    queue: Queue[Path] = Queue()
    queue.put(p)

    while not queue.empty():
        file = queue.get()

        if file.is_dir():
            for child in file.iterdir():
                queue.put(child)

            continue

        # todo(ayush): follow the import graph instead
        assert file.is_file()
        if file.suffix != ".py":
            continue

        v = Visitor(file)

        try:
            parsed = ast.parse(file.read_text(), filename=file)
        except SyntaxError as e:
            traceback.print_exc()
            click.secho(
                "\nRegistration failed due to a syntax error (see above)", fg="red"
            )
            raise click.exceptions.Exit(1) from e

        v.visit(parsed)

        res.extend(v.flyte_objects)

    return res
