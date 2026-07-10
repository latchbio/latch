from pathlib import Path

from latch_cli.centromere.ast_parsing import get_flyte_objects
from latch_cli.services.register.register import (
    _replace_task_protos,  # noqa: PLC2701
)

WORKFLOW_SOURCE = """\
from latch.resources.tasks import custom_task
from latch.resources.workflow import workflow


@custom_task(cpu=2, memory=4, storage_gib=10, dockerfile="Dockerfile.slim")
def light_task(x: int) -> int:
    return x


@workflow
def foo(x: int = 1) -> int:
    return light_task(x=x)
"""


def test_custom_image_replaces_dotted_module_task_protobuf(
    tmp_path: Path,
):
    workflow_module = tmp_path / "wf" / "foo"
    workflow_module.mkdir(parents=True)
    (workflow_module / "__init__.py").write_text(WORKFLOW_SOURCE)

    task = next(
        obj for obj in get_flyte_objects(workflow_module) if obj.type == "task"
    )

    assert task.name == "foo.__init__.light_task"

    default_task_proto = tmp_path / "default" / f"wf.{task.name}.pb"
    default_other_proto = tmp_path / "default" / "wf.foo.__init__.other_task.pb"
    custom_task_proto = tmp_path / "custom" / f"wf.{task.name}.pb"
    custom_other_proto = tmp_path / "custom" / "wf.foo.__init__.other_task.pb"

    assert _replace_task_protos(
        [default_task_proto, default_other_proto],
        [custom_task_proto, custom_other_proto],
        task.name,
    ) == [custom_task_proto, default_other_proto]


def test_custom_image_replaces_single_component_module_task_protobuf(tmp_path: Path):
    task_name = "wf.__init__.light_task"
    default_task_proto = tmp_path / "default" / f"{task_name}.pb"
    custom_task_proto = tmp_path / "custom" / f"{task_name}.pb"

    assert _replace_task_protos(
        [default_task_proto], [custom_task_proto], task_name
    ) == [custom_task_proto]
