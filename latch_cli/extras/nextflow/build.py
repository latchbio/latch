import glob
import os
import shutil
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional

import boto3
import click
from flytekit.core import constants as _common_constants
from flytekit.core.interface import transform_variable_map
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise
from flytekit.models import literals as literals_models

from latch_cli import tinyrequests

from ...click_utils import italic
from ...menus import select_tui
from ...utils import identifier_from_str
from ..common.serialize import binding_from_python
from ..common.utils import reindent
from .dag import DAG, VertexType
from .tasks.adapters import (
    NextflowProcessPostAdapterTask,
    NextflowProcessPreAdapterTask,
)
from .tasks.conditional import NextflowConditionalTask
from .tasks.input import NextflowInputTask
from .tasks.map import MapContainerTask
from .tasks.merge import NextflowMergeTask
from .tasks.operator import NextflowOperatorTask
from .tasks.output import NextflowOutputTask
from .tasks.process import NextflowProcessTask
from .workflow import NextflowWorkflow


def get_node_name(vertex_id: str) -> str:
    return f"n{vertex_id}"


def build_from_nextflow_dag(wf: NextflowWorkflow):
    global_start_node = Node(
        id=_common_constants.GLOBAL_INPUT_NODE_ID,
        metadata=None,
        bindings=[],
        upstream_nodes=[],
        flyte_entity=None,
    )

    interface_inputs = transform_variable_map(wf.python_interface.inputs)

    global_wf_inputs = {f"wf_{k}": v for k, v in wf.python_interface.inputs.items()}
    global_wf_input_bindings = [
        binding_from_python(
            var_name=f"wf_{k}",
            expected_literal_type=interface_inputs[k].type,
            t_value=Promise(
                var=k,
                val=NodeOutput(node=global_start_node, var=k),
            ),
            t_value_type=interface_inputs[k],
        )
        for k in wf.python_interface.inputs.keys()
    ]

    node_map: Dict[str, Node] = {}
    extra_nodes: List[Node] = []

    for vertex in wf.dag.toposorted():
        upstream_nodes = [global_start_node]

        task_inputs = OrderedDict({**global_wf_inputs})

        if len(vertex.outputNames) > 0:
            task_outputs = OrderedDict({o: Optional[str] for o in vertex.outputNames})
        else:
            task_outputs = {"res": Optional[str]}

        task_bindings: List[literals_models.Binding] = [*global_wf_input_bindings]
        branches: Dict[str, bool] = {}
        for dep, edge in wf.dag.ancestors()[vertex]:
            if dep.type == VertexType.Conditional:
                input_name = f"condition_{dep.id}"
                task_inputs[input_name] = Optional[bool]

                assert edge.branch is not None

                branches[input_name] = edge.branch

                node = NodeOutput(node=node_map[dep.id], var=f"condition")
            else:
                input_name = f"channel_{dep.id}"
                output_name = "res"

                if len(dep.outputNames) > 0:
                    idx = int(edge.label)
                    input_name = output_name = dep.outputNames[idx]

                task_inputs[input_name] = Optional[str]

                node = NodeOutput(node=node_map[dep.id], var=output_name)

            task_bindings.append(
                literals_models.Binding(
                    var=input_name,
                    binding=literals_models.BindingData(
                        promise=Promise(
                            var=input_name,
                            val=node,
                        ).ref
                    ),
                )
            )

            upstream_nodes.append(node_map[dep.id])

        node_name = get_node_name(vertex.id)

        if vertex.type == VertexType.Process:
            pre_adapter_task = NextflowProcessPreAdapterTask(
                inputs=task_inputs,
                id=f"{vertex.id}_pre",
                name=f"pre_adapter_{identifier_from_str(vertex.label)}",
                branches=branches,
                wf=wf,
            )
            wf.nextflow_tasks.append(pre_adapter_task)

            pre_adapter_node = Node(
                id=f"{node_name}preadapter",
                metadata=pre_adapter_task.construct_node_metadata(),
                bindings=sorted(task_bindings, key=lambda b: b.var),
                upstream_nodes=upstream_nodes,
                flyte_entity=pre_adapter_task,
            )
            extra_nodes.append(pre_adapter_node)

            post_adapter_task = NextflowProcessPostAdapterTask(
                outputs=task_outputs,
                id=f"{vertex.id}_post",
                name=f"post_adapter_{identifier_from_str(vertex.label)}",
                wf=wf,
            )

            wf.nextflow_tasks.append(post_adapter_task)

            process_task = NextflowProcessTask(
                inputs={"default": pre_adapter_task.dataclass},
                outputs={"o0": post_adapter_task.dataclass},
                id=vertex.id,
                name=identifier_from_str(vertex.label),
                statement=vertex.statement,
                ret=vertex.ret,
                import_path=Path(vertex.module),
                process_name=vertex.label,
                unaliased=vertex.unaliased,
                wf=wf,
            )

            wf.nextflow_tasks.append(process_task)

            mapped_process_task = MapContainerTask(process_task)
            mapped_process_node = Node(
                id=node_name,
                metadata=mapped_process_task.construct_node_metadata(),
                bindings=[
                    literals_models.Binding(
                        var="default",
                        binding=literals_models.BindingData(
                            promise=Promise(
                                var="default",
                                val=NodeOutput(
                                    node=pre_adapter_node,
                                    var="default",
                                ),
                            ).ref
                        ),
                    )
                ],
                upstream_nodes=[pre_adapter_node],
                flyte_entity=mapped_process_task,
            )

            extra_nodes.append(mapped_process_node)

            post_adapter_node = Node(
                id=f"{node_name}postadapter",
                metadata=post_adapter_task.construct_node_metadata(),
                bindings=[
                    literals_models.Binding(
                        var="default",
                        binding=literals_models.BindingData(
                            promise=Promise(
                                var="default",
                                val=NodeOutput(
                                    node=mapped_process_node,
                                    var="o0",
                                ),
                            ).ref
                        ),
                    )
                ],
                upstream_nodes=[mapped_process_node],
                flyte_entity=post_adapter_task,
            )

            node_map[vertex.id] = post_adapter_node

        elif vertex.type in VertexType.Conditional:
            conditional_task = NextflowConditionalTask(
                task_inputs,
                vertex.id,
                f"conditional_{vertex.label}",
                vertex.statement,
                vertex.ret,
                branches,
                wf,
            )
            wf.nextflow_tasks.append(conditional_task)

            node = Node(
                id=node_name,
                metadata=conditional_task.construct_node_metadata(),
                bindings=task_bindings,
                upstream_nodes=upstream_nodes,
                flyte_entity=conditional_task,
            )

            node_map[vertex.id] = node

        elif vertex.type == VertexType.Input:
            input_task = NextflowInputTask(
                inputs=task_inputs,
                outputs=task_outputs,
                name=vertex.label,
                id=vertex.id,
                branches=branches,
                wf=wf,
            )
            wf.nextflow_tasks.append(input_task)

            node = Node(
                id=node_name,
                metadata=input_task.construct_node_metadata(),
                bindings=task_bindings,
                upstream_nodes=upstream_nodes,
                flyte_entity=input_task,
            )

            node_map[vertex.id] = node

        elif vertex.type == VertexType.Output:
            output_task = NextflowOutputTask(
                inputs=task_inputs,
                outputs=task_outputs,
                name=vertex.label,
                id=vertex.id,
                statement=vertex.statement,
                ret=vertex.ret,
                branches=branches,
                wf=wf,
            )
            wf.nextflow_tasks.append(output_task)

            node = Node(
                id=node_name,
                metadata=output_task.construct_node_metadata(),
                bindings=task_bindings,
                upstream_nodes=upstream_nodes,
                flyte_entity=output_task,
            )

            node_map[vertex.id] = node

        elif vertex.type == VertexType.Merge:
            merge_task = NextflowMergeTask(
                inputs=task_inputs,
                outputs=task_outputs,
                name=vertex.label,
                id=vertex.id,
                branches=branches,
                wf=wf,
            )

            wf.nextflow_tasks.append(merge_task)

            node = Node(
                id=node_name,
                metadata=merge_task.construct_node_metadata(),
                bindings=task_bindings,
                upstream_nodes=upstream_nodes,
                flyte_entity=merge_task,
            )

            node_map[vertex.id] = node

        else:
            operator_task = NextflowOperatorTask(
                inputs=task_inputs,
                outputs=task_outputs,
                name=vertex.label,
                id=vertex.id,
                statement=vertex.statement,
                ret=vertex.ret,
                branches=branches,
                wf=wf,
            )
            wf.nextflow_tasks.append(operator_task)

            node = Node(
                id=node_name,
                metadata=operator_task.construct_node_metadata(),
                bindings=task_bindings,
                upstream_nodes=upstream_nodes,
                flyte_entity=operator_task,
            )

            node_map[vertex.id] = node

    wf._nodes = list(node_map.values()) + extra_nodes


# todo(ayush): add versioning system to nf download
# todo(ayush): allow user to redownload nf anyway via cli option
def ensure_nf_dependencies(pkg_root: Path, *, force_redownload: bool = False):
    try:
        subprocess.run(["java", "--version"], check=True, capture_output=True)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        click.secho(
            dedent("""\
            Java is not installed - this is a requirement to run Nextflow.

            Please install Java and try again.
            """),
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    nf_executable = pkg_root / ".latch" / "bin" / "nextflow"
    nf_jars = pkg_root / ".latch" / ".nextflow"

    if force_redownload:
        nf_executable.unlink(missing_ok=True)
        if nf_jars.exists():
            shutil.rmtree(nf_jars)

    if not nf_executable.exists():
        res = tinyrequests.get(
            "https://latch-public.s3.us-west-2.amazonaws.com/nextflow"
        )
        nf_executable.parent.mkdir(parents=True, exist_ok=True)

        nf_executable.write_bytes(res.content)
        nf_executable.chmod(0o700)

    if not nf_jars.exists():
        click.secho(
            "  Downloading Nextflow binaries: \x1b[?25l",
            italic=True,
            nl=False,
        )

        s3_resource = boto3.resource("s3")
        bucket = s3_resource.Bucket("latch-public")

        objects = list(bucket.objects.filter(Prefix=".nextflow/"))

        # todo(ayush): parallelize
        for i, obj in enumerate(objects):
            obj_path = pkg_root / ".latch" / obj.key
            obj_path.parent.mkdir(parents=True, exist_ok=True)

            bucket.download_file(obj.key, str(obj_path))

            progress_str = f"{i + 1}/{len(objects)}"

            click.echo("\x1b[0K", nl=False)
            click.secho(progress_str, dim=True, italic=True, nl=False)
            click.echo(f"\x1b[{len(progress_str)}D", nl=False)

        click.echo("\x1b[0K", nl=False)
        click.secho("Done. \x1b[?25h", italic=True)


def build_nf_wf(
    pkg_root: Path,
    nf_script: Path,
    *,
    redownload_dependencies: bool = False,
) -> NextflowWorkflow:
    ensure_nf_dependencies(pkg_root, force_redownload=redownload_dependencies)

    # clear out old dags from previous registers
    old_dag_files = map(Path, glob.glob(str(pkg_root / ".latch" / "*.dag.json")))
    for f in old_dag_files:
        f.unlink()

    env = {
        **os.environ,
        # read NF binaries from `.latch/.nextflow` instead of system
        "NXF_HOME": str(pkg_root / ".latch" / ".nextflow"),
        # don't display version mismatch warning
        "NXF_DISABLE_CHECK_LATEST": "true",
        # don't emit .nextflow.log files
        "NXF_LOG_FILE": "/dev/null",
    }

    if os.environ.get("LATCH_NEXTFLOW_DEV") is not None:
        env = os.environ

    try:
        subprocess.run(
            [
                str(pkg_root / ".latch" / "bin" / "nextflow"),
                "-quiet",
                "run",
                str(nf_script.resolve()),
                "-latchRegister",
            ],
            env=env,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        click.secho(
            reindent(
                f"""
                An error occurred while parsing your NF script ({italic(nf_script)})
                Check your script for typos.

                Contact support@latch.bio for help if the issue persists.\
                """,
                0,
            ),
            fg="red",
        )
        raise click.exceptions.Exit(1) from e

    dags: Dict[str, DAG] = {}

    dag_files = map(Path, glob.glob(".latch/*.dag.json"))
    main_dag: Optional[DAG] = None
    for dag in dag_files:
        wf_name = dag.name.rsplit(".", 2)[0]

        dags[wf_name] = DAG.from_path(dag)
        if wf_name == "mainWorkflow":
            main_dag = dags[wf_name]

    if len(dags) == 0:
        click.secho("No Nextflow workflows found in this project. Aborting.", fg="red")

        raise click.exceptions.Exit(1)

    if main_dag is None:
        main_dag = select_tui(
            "We found multiple target workflows in this Nextflow project. Which"
            " would you like to register?",
            [{"display_name": k, "value": v} for k, v in dags.items()],
        )

        if main_dag is None:
            click.echo("No workflow selected. Aborting.")

            raise click.exceptions.Exit(0)

    wf = NextflowWorkflow(nf_script, main_dag)

    build_from_nextflow_dag(wf)

    return wf


def generate_nf_entrypoint(
    wf: NextflowWorkflow,
    pkg_root: Path,
    nf_script: Path,
):
    preamble = reindent(
        r"""
        import glob
        import json
        import os
        import re
        import shutil
        import stat
        import subprocess
        import sys
        import time
        import traceback
        import typing
        from dataclasses import asdict, dataclass, fields, is_dataclass
        from enum import Enum
        from itertools import chain, repeat
        from pathlib import Path
        from subprocess import CalledProcessError
        from typing import Dict, List, NamedTuple

        from flytekit.extras.persistence import LatchPersistence
        from latch_cli.extras.nextflow.file_persistence import download_files, stage_for_output, upload_files
        from latch_cli.extras.nextflow.channel import get_mapper_inputs
        from latch_cli.utils import check_exists_and_rename, get_parameter_json_value, urljoins

        from latch.resources.tasks import custom_task
        from latch.types.directory import LatchDir, LatchOutputDir
        from latch.types.file import LatchFile

        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)

        task = custom_task(cpu=-1, memory=-1) # these limits are a lie and are ignored when generating the task spec

        """,
        0,
    )

    nf_script_path_in_container = nf_script.resolve().relative_to(pkg_root.resolve())

    entrypoint_code = [preamble]
    for task in wf.nextflow_tasks:
        entrypoint_code.append(task.get_fn_code(nf_script_path_in_container))

    entrypoint = pkg_root / ".latch" / "nf_entrypoint.py"
    entrypoint.write_text("\n\n".join(entrypoint_code))
