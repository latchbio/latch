from pathlib import Path
from typing import Dict, List, Mapping, Type

from latch.types.metadata import ParameterType

from ....extras.common.utils import reindent
from ..workflow import NextflowWorkflow
from .operator import NextflowOperatorTask


class NextflowInputTask(NextflowOperatorTask):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, "", [], branches, wf)

        assert len(self.channel_inputs) == 1, (
            self.channel_inputs,
            self.conditional_inputs,
            self.wf_inputs,
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

        channel_input = self.channel_inputs.popitem()[0]

        code_block += reindent(
            rf"""
                res = {channel_input}
            else:
                print("TASK SKIPPED")
                res = None

            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block
