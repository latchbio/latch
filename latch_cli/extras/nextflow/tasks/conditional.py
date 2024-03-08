from typing import Dict, List, Mapping, Optional, Type

from latch.types.metadata import ParameterType

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
            wf,
        )

    def get_fn_return_stmt(self):
        results: List[str] = []
        for out_name, out_type in self._python_outputs.items():
            results.append(
                reindent(
                    rf"""
                    {out_name}=json.loads(out_channels.get("{out_name}", "true"))
                    """,
                    2,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
            res = out_channels.get({repr(out_name)})

            if res is not None:
                res = json.loads(res)[0]["boolean"]

            return Res{self.name}(condition=res)
            """,
            1,
        ).replace("__return_str__", return_str)
