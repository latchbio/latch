import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Type

from flytekit.configuration import SerializationSettings

from latch.types.metadata import ParameterType

from ...common.utils import is_blob_type, reindent, type_repr
from ..workflow import NextflowWorkflow
from .base import NextflowBaseTask, NFTaskType


class NextflowProcessTask(NextflowBaseTask):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        statement: str,
        ret: List[str],
        script_path: Path,
        calling_subwf_name: str,
        process_name: str,
        unaliased: str,
        execution_profile: Optional[str],
        wf: NextflowWorkflow,
    ):
        super().__init__(
            inputs, outputs, id, name, {}, wf, NFTaskType.Process, cpu=16, memory=48
        )

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
        self.script_path = script_path
        self.calling_subwf_name = calling_subwf_name
        self.process_name = process_name
        self.unaliased = unaliased
        self.execution_profile = execution_profile

    def get_custom(self, _: SerializationSettings) -> Dict[str, Any]:
        return {"preExecEnabled": True, "useDynamicResources": True}

    def get_fn_interface(self, nf_script_path_in_container: Path):
        input_name, input_t = list(self._python_inputs.items())[0]
        output_t = list(self._python_outputs.values())[0]

        run_task_entrypoint = [
            "/root/nextflow",
            "run",
            str(nf_script_path_in_container),
        ]

        return reindent(
            rf"""
                def _read_resources() -> Dict:
                    try:
                        with open(".latch/resources.json") as f:
                            return json.load(f)
                    except FileNotFoundError:
                        return {{}}

                def allocate_cpu({input_name}: {type_repr(input_t)}) -> int:
                    res = _read_resources()
                    return max(1, res['cpu_cores']) if res.get('cpu_cores') is not None else None

                def allocate_memory({input_name}: {type_repr(input_t)}) -> int:
                    res = _read_resources()
                    return max(1, res['memory_bytes'] // 1024**3) if res.get('memory_bytes') is not None else None

                def allocate_disk({input_name}: {type_repr(input_t)}) -> int:
                    res = _read_resources()
                    return max(1, res['disk_bytes'] // 1024**3) if res.get('disk_bytes') is not None else None

                def get_resources({input_name}: {type_repr(input_t)}):
                    try:
                        subprocess.run(
                            [{','.join([f"str({x})" if x.startswith("wf_") else repr(x) for x in run_task_entrypoint])}],
                            env={{
                                **os.environ,
                                "LATCH_EXPRESSION": {repr(self.statement)},
                                "LATCH_PRE_EXECUTE": 'True',
                            }},
                            check=True,
                        )
                    except subprocess.CalledProcessError:
                        log = Path("/root/.nextflow.log").read_text()
                        print("\n\n\n\n\n" + log)

                @custom_task(cpu=allocate_cpu, memory=allocate_memory, storage_gib=allocate_disk, pre_execute=get_resources, cache=True)
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
        code_block = self.get_fn_interface(nf_script_path_in_container)

        code_block += reindent(
            """
            wf_paths = {}
            """,
            1,
        )

        run_task_entrypoint = [
            "/root/nextflow",
            "run",
            str(nf_script_path_in_container),
        ]

        if self.execution_profile is not None:
            run_task_entrypoint.extend(["-profile", self.execution_profile])

        for flag, val in self.wf.flags_to_params.items():
            run_task_entrypoint.extend([flag, str(val)])

        for k, typ in self.wf_inputs.items():
            code_block += reindent(
                f"""
                {k} = default.{k}
                """,
                1,
            )

            if k[3:] in self.wf.downloadable_params:
                code_block += reindent(
                    f"""
                    if {k} is not None:
                        {k}_p = Path({k}).resolve()
                        check_exists_and_rename({k}_p, Path("/root") / {k}_p.name)
                        wf_paths["{k}"] = Path("/root") / {k}_p.name

                    """,
                    1,
                )
            elif is_blob_type(typ):
                code_block += reindent(
                    f"""
                    if {k} is not None:
                        {k}_p = Path("/root/").resolve() # superhack
                        wf_paths["{k}"] = {k}_p

                    """,
                    1,
                )

        if self.script_path.resolve() != self.wf.nf_script.resolve():
            stem = self.script_path.resolve().relative_to(self.wf.pkg_root.resolve())
            run_task_entrypoint[2] = str(Path("/root") / stem)
            run_task_entrypoint.extend(["-entry", self.calling_subwf_name])

        code_block += reindent(
            rf"""

            channel_vals = [{','.join([f"json.loads(default.{x})" for x in self.channel_inputs])}]

            download_files(channel_vals, LatchDir({repr(self.wf.output_directory.remote_path)}))

            try:
                subprocess.run(
                    [{','.join([f"str({x})" if x.startswith("wf_") else repr(x) for x in run_task_entrypoint])}],
                    env={{
                        **os.environ,
                        "LATCH_EXPRESSION": {repr(self.statement)},
                        "LATCH_RETURN": {repr(json.dumps(self.ret))},
                        "LATCH_PARAM_VALS": json.dumps(channel_vals),
                    }},
                    check=True,
                )
            except subprocess.CalledProcessError:
                log = Path("/root/.nextflow.log").read_text()
                print("\n\n\n\n\n" + log)
                raise

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
