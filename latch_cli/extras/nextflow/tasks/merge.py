from pathlib import Path
from typing import Dict, List, Mapping, Type

from latch.types.metadata import ParameterType
from latch_cli.extras.nextflow.tasks.base import NFTaskType

from ....extras.common.utils import reindent
from ..workflow import NextflowWorkflow
from .operator import NextflowOperatorTask


class NextflowMergeTask(NextflowOperatorTask):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        branches: Dict[str, bool],
        sources: Dict[str, List[str]],
        script_path: Path,
        calling_subwf_name: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(
            inputs,
            outputs,
            id,
            name,
            "",
            [],
            branches,
            script_path,
            calling_subwf_name,
            wf,
        )

        self.sources = sources
        self.nf_task_type = NFTaskType.Merge

    def get_fn_conditions(self):
        res: List[str] = []
        for k in self.conditional_inputs.keys():
            res.append(f"({k} == {self.branches[k]})")

        if len(res) == 0:
            return reindent(
                f"""\
                cond = True

                if cond:
                """,
                1,
            )

        return reindent(
            f"""\
            cond = ({' and '.join(res)})

            if cond:
            """,
            1,
        )

    def get_fn_return_stmt(self):
        results: List[str] = []
        for out_name in self._python_outputs.keys():
            results.append(
                reindent(rf"{out_name}=res.get({repr(out_name)})", 2).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                return Res{self.name}(
            ||return|str||
                )
            """,
            0,
        ).replace("||return|str||", return_str)

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_conditions()

        exprs = []
        for out_name, inputs in self.sources.items():
            expr = " or ".join(input for input in inputs)

            exprs.append(f"{repr(out_name)}: {expr}")

        expr = ", ".join(x for x in exprs)

        code_block += reindent(
            rf"""
                res = {{ {expr} }}
            else:
                print("TASK SKIPPED")
                try:
                    _override_task_status(status="SKIPPED")
                except Exception as e:
                    print(f"Failed to override task status: {{e}}")
                res = {{}}

            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block
