import ast
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import Literal, Optional

import click


@dataclass
class FlyteObject:
    type: Literal["task", "workflow"]
    name: str
    dockerfile: Optional[Path] = None


def is_task_decorator(decorator_name: str) -> bool:
    return decorator_name in {
        # og
        "small_task",
        "medium_task",
        "large_task",
        # og gpu
        "small_gpu_task",
        "large_gpu_task",
        # custom
        "custom_task",
        "custom_memory_optimized_task",
        # nf
        "nextflow_runtime_task",
        # l40s gpu
        "g6e_xlarge_task",
        "g6e_2xlarge_task",
        "g6e_4xlarge_task",
        "g6e_8xlarge_task",
        "g6e_12xlarge_task",
        "g6e_16xlarge_task",
        "g6e_24xlarge_task",
        # v100 gpu
        "v100_x1_task",
        "v100_x4_task",
        "v100_x8_task",
    }


class Visitor(ast.NodeVisitor):
    def __init__(self, file: Path):
        self.file = file
        self.flyte_objects: list[FlyteObject] = []

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if len(node.decorator_list) == 0:
            return self.generic_visit(node)

        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name):
                if decorator.id == "workflow":
                    self.flyte_objects.append(FlyteObject("workflow", node.name))
                elif is_task_decorator(decorator.id):
                    self.flyte_objects.append(FlyteObject("task", node.name))

            elif isinstance(decorator, ast.Call):
                func = decorator.func
                assert isinstance(func, ast.Name)

                if not is_task_decorator(func.id) and func.id != "workflow":
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

    v = Visitor(file.resolve())

    try:
        parsed = ast.parse(file.read_text(), filename=file)
    except SyntaxError as e:
        click.secho(f"There is a syntax error in {file}: {e}", fg="red")
        raise click.exceptions.Exit(1) from e

    v.visit(parsed)

    return v.flyte_objects
