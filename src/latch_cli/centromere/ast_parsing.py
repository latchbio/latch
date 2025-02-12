import ast
from dataclasses import dataclass
from pathlib import Path
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
        self.flyte_objects: list[FlyteObject] = []

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if len(node.decorator_list) == 0:
            return self.generic_visit(node)

        # todo(ayush):
        # 1. support ast.Attribute (@latch.tasks.small_task)
        # 2. normalize to fqn before checking whether or not its a task decorator
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name):
                if decorator.id == "workflow":
                    self.flyte_objects.append(FlyteObject("workflow", node.name))
                elif decorator.id in task_decorators:
                    self.flyte_objects.append(FlyteObject("task", node.name))

            elif isinstance(decorator, ast.Call):
                func = decorator.func
                assert isinstance(func, ast.Name)

                if func.id not in task_decorators and func.id != "workflow":
                    continue

                if func.id == "workflow":
                    self.flyte_objects.append(FlyteObject("workflow", node.name))
                    continue

                dockerfile: Optional[Path] = None
                for kw in decorator.keywords:
                    if kw.arg != "dockerfile":
                        continue

                    try:
                        dockerfile = Path(ast.literal_eval(kw.value)).resolve()
                    except ValueError as e:
                        click.secho(
                            dedent(f"""\
                                There was an issue parsing the `dockerfile` argument for task `{node.name}` in {self.file}.
                                Note that values passed to `dockerfile` must be string literals.
                            """),
                            fg="red",
                        )

                        raise click.exceptions.Exit(1) from e

                    if not dockerfile.exists():
                        click.secho(
                            f"The `dockerfile` value ({dockerfile}) for task `{node.name}` in {self.file} does not exist.",
                            fg="red",
                        )

                        raise click.exceptions.Exit(1)

                self.flyte_objects.append(FlyteObject("task", node.name, dockerfile))

        return self.generic_visit(node)


def get_flyte_objects(file: Path) -> list[FlyteObject]:
    res = []
    if file.is_dir():
        for child in file.iterdir():
            res.extend(get_flyte_objects(child))

        return res

    assert file.is_file()
    if file.suffix != ".py":
        return res

    v = Visitor(file)

    try:
        parsed = ast.parse(file.read_text(), filename=file)
    except SyntaxError as e:
        click.secho(f"There is a syntax error in {file}: {e}", fg="red")
        raise click.exceptions.Exit(1) from e

    v.visit(parsed)

    return v.flyte_objects
