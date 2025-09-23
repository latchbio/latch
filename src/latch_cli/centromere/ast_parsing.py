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
    def __init__(self, file: Path, module_name: str):
        self.file = file
        self.module_name = module_name
        self.flyte_objects: list[FlyteObject] = []

    # todo(ayush): skip defs that arent run on import
    def visit_FunctionDef(self, node: ast.FunctionDef):  # noqa: N802
        if len(node.decorator_list) == 0:
            return self.generic_visit(node)

        fqn = f"{self.module_name}.{node.name}"

        # todo(ayush): |
        # 1. support ast.Attribute (@latch.tasks.small_task)
        # 2. normalize to fqn before checking whether or not its a task decorator
        # 3. save fully qualified name for tasks (need to parse based on import graph)
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name):
                if decorator.id in {"workflow", "nextflow_workflow"}:
                    self.flyte_objects.append(FlyteObject("workflow", fqn))
                elif decorator.id in task_decorators:
                    self.flyte_objects.append(FlyteObject("task", fqn))

            elif isinstance(decorator, ast.Call):
                func = decorator.func
                assert isinstance(func, ast.Name)

                if func.id not in task_decorators and func.id not in {
                    "workflow",
                    "nextflow_workflow",
                }:
                    continue

                if func.id in {"workflow", "nextflow_workflow"}:
                    self.flyte_objects.append(FlyteObject("workflow", fqn))
                    continue

                # note(ayush): this only works if `dockerfile` is a keyword arg - if someone
                # is insane enough to pass in the 14 other arguments first then have `dockerfile`
                # as a positional arg i will fix it
                dockerfile: Optional[Path] = None
                for kw in decorator.keywords:
                    if kw.arg != "dockerfile":
                        continue

                    try:
                        dockerfile = Path(ast.literal_eval(kw.value))
                    except ValueError as e:
                        click.secho(
                            dedent(f"""\
                                There was an issue parsing the `dockerfile` argument for task `{fqn}` in {self.file}.
                                Note that values passed to `dockerfile` must be string literals.
                            """),
                            fg="red",
                        )

                        raise click.exceptions.Exit(1) from e

                self.flyte_objects.append(FlyteObject("task", fqn, dockerfile))

        return self.generic_visit(node)


def get_flyte_objects(module: Path) -> list[FlyteObject]:
    res: list[FlyteObject] = []
    queue: Queue[Path] = Queue()
    queue.put(module)

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

        module_name = str(file.with_suffix("").relative_to(module.parent)).replace(
            os.sep, "."
        )

        v = Visitor(file, module_name)

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
