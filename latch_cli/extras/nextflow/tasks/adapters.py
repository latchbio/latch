import typing
from dataclasses import Field, dataclass, fields, make_dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Type, Union, get_args, get_origin

from latch.types.metadata import ParameterType, _IsDataclass

from ...common.utils import reindent, type_repr
from ..workflow import NextflowWorkflow
from .base import NextflowBaseTask


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
        __output_fields__

        """,
        0,
    ).replace("__output_fields__", output_fields)


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
            inputs, {"default": List[self.dataclass]}, id, name, branches, wf
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

            """,
            0,
        )

        res += reindent(
            rf"""
                @task(cache=True)
                def {self.name}(
                __params__
                ) -> Res_{self.id}:
                """,
            0,
        ).replace("__params__", params_str)

        return res

    def get_fn_return_stmt(self):
        return reindent(f"return Res_{self.id}(default=result)", 1)

    def get_fn_code(self, nf_path_in_container: str):
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

        super().__init__({"default": List[self.dataclass]}, outputs, id, name, {}, wf)

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
            __output_fields__

            """,
            0,
        ).replace("__output_fields__", output_fields)

        res += get_dataclass_code(self.dataclass)

        res += reindent(
            rf"""
            @task(cache=True)
            def {self.name}(
                default: List[Dataclass_{self.id}]
            ) -> Res{self.name}:
            """,
            0,
        )

        return res

    def get_fn_return_stmt(self):
        results: List[str] = []
        for out_name in self._python_outputs.keys():
            results.append(
                reindent(
                    rf"""
                    {out_name}=json.dumps([*chain.from_iterable([json.loads(x.{out_name}) for x in default])])
                    """,
                    1,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
            return Res{self.name}(
            __return_str__
            )
            """,
            1,
        ).replace("__return_str__", return_str)

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = self.get_fn_interface()
        code_block += self.get_fn_return_stmt()
        return code_block
