from pathlib import Path
from typing import Dict, List, Mapping, Optional, Type

from latch.types.metadata import ParameterType
from latch_cli.extras.nextflow.tasks.base import NFTaskType

from ...common.utils import reindent
from ..workflow import NextflowWorkflow
from .operator import NextflowOperatorTask


class NextflowConditionalTask(NextflowOperatorTask):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        statement: str,
        ret: List[str],
        branches: Dict[str, bool],
        script_path: Path,
        calling_subwf_name: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(
            inputs,
            {"condition": Optional[bool]},
            id,
            name,
            statement,
            ret,
            branches,
            script_path,
            calling_subwf_name,
            wf,
        )

        self.nf_task_type = NFTaskType.Conditional

    def get_fn_return_stmt(self):
        return reindent(
            rf"""
            res = out_channels.get("condition")

            if res is not None:
                res = get_boolean_value(res)

            return Res{self.name}(condition=res)
            """,
            1,
        )
