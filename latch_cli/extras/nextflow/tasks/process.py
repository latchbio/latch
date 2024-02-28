import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import List, Mapping, Type

from latch.types.metadata import ParameterType

from ...common.utils import reindent, type_repr
from ..workflow import NextflowWorkflow
from .base import NextflowBaseTask


class NextflowProcessTask(NextflowBaseTask):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        statement: str,
        ret: List[str],
        import_path: Path,
        process_name: str,
        unaliased: str,
        wf: NextflowWorkflow,
    ):
        super().__init__(inputs, outputs, id, name, {}, wf)

        self.wf_inputs = {}
        self.conditional_inputs = {}
        self.channel_inputs = {}

        assert is_dataclass(inputs["default"])

        for f in fields(inputs["default"]):
            k = f.name
            v = f.type

            if k.startswith("wf_"):
                self.wf_inputs[k] = v
            elif k.startswith("condition_"):
                self.conditional_inputs[k] = v
            else:
                self.channel_inputs[k] = v

        self.statement = statement
        self.ret = ret
        self.import_path = import_path
        self.process_name = process_name
        self.unaliased = unaliased

    def get_fn_interface(self):
        input_name, input_t = list(self._python_inputs.items())[0]
        output_t = list(self._python_outputs.values())[0]

        return reindent(
            rf"""
                @task(cache=True)
                def {self.name}(
                    {input_name}: {type_repr(input_t)}
                ) -> {type_repr(output_t)}:
                """,
            0,
        )

    def get_fn_return_stmt(self):
        results: List[str] = []

        res_type = list(self._python_outputs.values())[0]

        if res_type is None:
            return "    return None"

        assert is_dataclass(res_type)

        for field in fields(res_type):
            results.append(
                reindent(
                    rf"""
                    {field.name}=out_channels.get(f"{field.name}", "")
                    """,
                    2,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                    return {res_type.__name__}(
                __return_str__
                    )
            """,
            0,
        ).replace("__return_str__", return_str)

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = self.get_fn_interface()

        run_task_entrypoint = [
            "/root/nextflow",
            "run",
            str(nf_script_path_in_container),
        ]

        for flag, val in self.wf.flags_to_params.items():
            run_task_entrypoint.extend([flag, str(val)])

        for k, v in self.wf.downloadable_params.items():
            code_block += reindent(
                f"""
                if default.wf_{k} is not None:
                    {k}_p = Path(default.wf_{k}).resolve()
                    {k}_dest_p = Path({repr(v)}).resolve()

                    check_exists_and_rename(
                        {k}_p,
                        {k}_dest_p
                    )

                """,
                1,
            )

        include_str = ""
        if self.import_path.resolve() != self.wf.nf_script.resolve():
            include_meta = {}
            stem = str(
                self.import_path.resolve().relative_to(
                    self.wf.nf_script.parent.resolve()
                )
            )
            include_meta["path"] = f"./{stem}"
            include_meta["alias"] = self.process_name
            include_meta["name"] = self.unaliased

            include_str = json.dumps(include_meta)

        code_block += reindent(
            rf"""

            channel_vals = [{','.join([f"json.loads(default.{x})" for x in self.channel_inputs])}]

            download_files(channel_vals, LatchDir({repr(self.wf.output_directory.remote_path)}))

            subprocess.run(
                [{','.join([f"str(default.{x})" if x.startswith("wf_") else repr(x) for x in run_task_entrypoint])}],
                env={{
                    **os.environ,
                    "LATCH_INCLUDE_META": {repr(include_str)},
                    "LATCH_EXPRESSION": {repr(self.statement)},
                    "LATCH_RETURN": {repr(json.dumps(self.ret))},
                    "LATCH_PARAM_VALS": json.dumps(channel_vals),
                }},
                check=True,
            )

            out_channels = {{}}
            files = [Path(f) for f in glob.glob(".latch/task-outputs/*.json")]

            for file in files:
                out_channels[file.stem] = file.read_text()

            print(out_channels)

            upload_files({{k: json.loads(v) for k, v in out_channels.items()}}, LatchDir({repr(self.wf.output_directory.remote_path)}))

            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block
