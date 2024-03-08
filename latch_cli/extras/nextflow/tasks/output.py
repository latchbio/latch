import json
from pathlib import Path
from typing import Dict, List, Mapping, Type

from latch.types.metadata import ParameterType

from ....extras.common.utils import reindent
from ..workflow import NextflowWorkflow
from .operator import NextflowOperatorTask


class NextflowOutputTask(NextflowOperatorTask):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        statement: str,
        ret: List[str],
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
    ):
        super().__init__(
            inputs,
            outputs,
            id,
            name,
            statement,
            ret,
            branches,
            wf,
        )

    def get_fn_return_stmt(self):
        assert len(self._python_outputs) == len(
            self.channel_inputs
        ), f"{self.name, self._python_outputs, self.channel_inputs}"

        results: List[str] = []
        for out_name, param_name in zip(
            self._python_outputs.keys(), self.channel_inputs.keys()
        ):
            results.append(
                reindent(rf"{out_name}=({param_name} if cond else None)", 2).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                return Res{self.name}(
            __return_str__
                )
            """,
            0,
        ).replace("__return_str__", return_str)

    def get_fn_conditions(self):
        res: List[str] = []
        for k in self.conditional_inputs.keys():
            res.append(f"({k} == {self.branches[k]})")
        for k in self.channel_inputs.keys():
            res.append(f"({k} is not None)")

        if len(res) == 0:
            return reindent(
                f"""\
                cond = True
                """,
                1,
            )

        return reindent(
            f"""\
            cond = ({' and '.join(res)})
            """,
            1,
        )

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_conditions()
        code_block += self.get_fn_return_stmt()

        return code_block
