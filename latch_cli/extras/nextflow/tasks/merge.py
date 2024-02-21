from pathlib import Path
from typing import Dict, List, Mapping, Type

from latch.types.metadata import ParameterType

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
            wf,
        )

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
            results.append(reindent(rf"{out_name}=res", 2).rstrip())

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                return Res{self.name}(
            __return_str__
                )
            """,
            0,
        ).replace("__return_str__", return_str)

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_conditions()

        expr = " or ".join(channel_input for channel_input in self.channel_inputs)

        code_block += reindent(
            rf"""
                res = {expr}
            else:
                print("TASK SKIPPED")
                res = None

            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block
