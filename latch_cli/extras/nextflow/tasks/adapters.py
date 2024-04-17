import typing
from dataclasses import Field, dataclass, fields, make_dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Type, Union, get_args, get_origin

from latch.types.metadata import ParameterType, _IsDataclass

from ...common.utils import reindent, type_repr
from ..workflow import NextflowWorkflow
from .base import NextflowBaseTask, NFTaskType


def dataclass_from_python_params(
    params: Mapping[str, Type[ParameterType]],
    name: str,
    *,
    unwrap_optionals: bool = True,
) -> Type[_IsDataclass]:
    fields = []
    for n, typ in params.items():
        if n.startswith("condition_"):
            continue

        if not unwrap_optionals or not (get_origin(typ) is Union):
            fields.append((n, typ))
            continue

        args = []
        for arg in get_args(typ):
            if arg is type(None):
                continue

            args.append(arg)

        fields.append((n, Union[tuple(args)]))

    return make_dataclass(cls_name=f"Dataclass_{name}", fields=fields)


def get_dataclass_code(cls: Type[_IsDataclass]) -> str:
    output_fields = "\n".join(
        reindent(f"{f.name}: {type_repr(f.type)}", 1) for f in fields(cls)
    )

    return reindent(
        rf"""
        @dataclass
        class {cls.__name__}:
        ||output|fields||

        """,
        0,
    ).replace("||output|fields||", output_fields)


class NextflowProcessPreAdapterTask(NextflowBaseTask):
    def __init__(
        self,
        inputs: Dict[str, Type[ParameterType]],
        id: str,
        name: str,
        branches: Dict[str, bool],
        wf: NextflowWorkflow,
    ):
        self.dataclass = dataclass_from_python_params(inputs, id)

        super().__init__(
            inputs,
            {"default": List[self.dataclass], "is_skipped": bool},
            id,
            name,
            branches,
            wf,
            NFTaskType.Adapter,
        )

    def get_fn_interface(self):
        res = ""

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: {type_repr(t)}
                """,
                1,
            ).rstrip()
            for param, t in self._python_inputs.items()
        )

        res += get_dataclass_code(self.dataclass)

        output_typ = self._python_outputs["default"]

        res += reindent(
            rf"""

            class Res_{self.id}(NamedTuple):
                default: {type_repr(output_typ)}
                is_skipped: bool

            """,
            0,
        )

        res += reindent(
            rf"""
                @task(cache=True)
                def {self.name}(
                ||params||
                ) -> Res_{self.id}:
                """,
            0,
        ).replace("||params||", params_str)

        return res

    def get_fn_return_stmt(self):
        return reindent(
            f"return Res_{self.id}(default=result, is_skipped = not cond)", 1
        )

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_conditions()

        fs = fields(self.dataclass)

        channel_fields: List[Field] = []
        wf_fields: List[Field] = []
        for f in fs:
            if f.name.startswith("wf_"):
                wf_fields.append(f)
            else:
                channel_fields.append(f)

        wf_dict_str = (
            "{" + ", ".join(f"{repr(f.name)}: {f.name}" for f in wf_fields) + "}"
        )
        channel_dict_str = (
            "{" + ", ".join(f"{repr(f.name)}: {f.name}" for f in channel_fields) + "}"
        )

        code_block += reindent(
            f"""
            result = get_mapper_inputs({self.dataclass.__name__}, {wf_dict_str}, {channel_dict_str})
            """,
            2,
        )

        code_block += reindent(
            rf"""
            else:
                print("TASK SKIPPED")
                try:
                    _override_task_status(status="SKIPPED")
                except Exception as e:
                    print(f"Failed to override task status: {{e}}")
                result = []

            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block


class NextflowProcessPostAdapterTask(NextflowBaseTask):
    def __init__(
        self,
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        wf: NextflowWorkflow,
    ):
        self.dataclass = dataclass_from_python_params(outputs, id)

        super().__init__(
            {"default": List[self.dataclass], "is_skipped": bool},
            outputs,
            id,
            name,
            {},
            wf,
            NFTaskType.Adapter,
        )

    def get_fn_interface(self):
        res = ""

        output_fields = "\n".join(
            reindent(
                rf"""
                {param}: {type_repr(t)}
                """,
                1,
            ).rstrip()
            for param, t in self._python_outputs.items()
        )

        res += reindent(
            rf"""
            class Res{self.name}(NamedTuple):
            ||output|fields||

            """,
            0,
        ).replace("||output|fields||", output_fields)

        res += get_dataclass_code(self.dataclass)

        res += reindent(
            rf"""
            @task(cache=True)
            def {self.name}(
                default: List[Dataclass_{self.id}],
                is_skipped: bool,
            ) -> Res{self.name}:
            """,
            0,
        )

        return res

    def get_fn_return_stmt(self):
        return reindent(
            rf"""
            return get_mapper_outputs(Res{self.name}, default, is_skipped)
            """,
            1,
        )

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_return_stmt()
        return code_block
