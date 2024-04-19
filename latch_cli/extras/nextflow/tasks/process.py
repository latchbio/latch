import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from textwrap import dedent
from typing import List, Mapping, Optional, Type

from latch.types.metadata import ParameterType

from ...common.utils import is_blob_type, is_samplesheet_param, reindent, type_repr
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
        cpu: Optional[int] = None,
        memory: Optional[float] = None,
        storage_gib: int = 500,
    ):
        super().__init__(
            inputs,
            outputs,
            id,
            name,
            {},
            wf,
            NFTaskType.Process,
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

    def _get_nextflow_fn_code(
        self, nf_script_path_in_container: Path, pre_execute: bool = False
    ):
        code_block = reindent(
            """
            wf_paths = {}
            """,
            0,
        )

        run_task_entrypoint = [
            "/root/nextflow",
            "run",
            str(nf_script_path_in_container),
            "-lib",
            "lib",
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
                0,
            )

            if k[3:] in self.wf.downloadable_params:
                code_block += reindent(
                    f"""
                    if {k} is not None:
                        if not {pre_execute}:
                            {k}_p = Path({k}).resolve()
                            check_exists_and_rename({k}_p, Path("/root") / {k}_p.name)
                            wf_paths["{k}"] = Path("/root") / {k}_p.name
                        else:
                            wf_paths["{k}"] = Path("/root") / "{k}"

                    """,
                    0,
                )
            elif is_blob_type(typ):
                code_block += reindent(
                    f"""
                    if {k} is not None:
                        if not {pre_execute}:
                            {k}_p = Path("/root/").resolve() # superhack
                            wf_paths["{k}"] = {k}_p
                        else:
                            wf_paths["{k}"] = Path("/root") / "{k}"

                    """,
                    0,
                )
            elif is_samplesheet_param(typ):
                code_block += reindent(
                    f"""
                    {k} = construct_samplesheet({k})
                    """,
                    0,
                )

        if self.script_path.resolve() != self.wf.nf_script.resolve():
            stem = self.script_path.resolve().relative_to(self.wf.pkg_root.resolve())
            run_task_entrypoint[2] = str(Path("/root") / stem)
            run_task_entrypoint.extend(["-entry", self.calling_subwf_name])

        # TODO (kenny) : only login if we need to
        if not pre_execute and self.wf.docker_metadata is not None:
            code_block += reindent(
                rf"""

                print("\n\n\nLogging into Docker\n")
                from latch.functions.secrets import get_secret
                docker_usr = "{self.wf.docker_metadata.username}"

                try:
                    docker_pwd = get_secret("{self.wf.docker_metadata.secret_name}")
                except ValueError as e:
                    print("Failed to get Docker credentials:", e)
                    sys.exit(1)

                login_cmd = [
                    "docker",
                    "login",
                    "--username",
                    docker_usr,
                    "--password",
                    docker_pwd,
                ]


                docker_server = "{self.wf.docker_metadata.server}"
                if docker_server != "None":
                    login_cmd.append(docker_server)

                try:
                    subprocess.run(
                        login_cmd,
                        check=True,
                    )
                except CalledProcessError as e:
                    print("Failed to login to Docker")
                except Exception:
                    traceback.print_exc()
                """,
                0,
            )

        code_block += reindent(
            rf"""

            channel_vals = [{','.join([f"json.loads(default.{x})" for x in self.channel_inputs])}]

            if not {pre_execute}:
                download_files(channel_vals, LatchDir({repr(self.wf.output_directory.remote_path)}))

            try:
                subprocess.run(
                    [{','.join([f"str({x})" if x.startswith("wf_") else repr(x) for x in run_task_entrypoint])}],
                    env={{
                        **os.environ,
                        "LATCH_BIN_DIR_OVERRIDE": str(Path.cwd() / "bin"),
                        "LATCH_CONFIG_DIR_OVERRIDE": str(Path.cwd()),
                        "LATCH_EXPRESSION": {repr(self.statement)},
                        "LATCH_RETURN": {repr(json.dumps(self.ret))},
                        "LATCH_PARAM_VALS": json.dumps(channel_vals),
                        "LATCH_PRE_EXECUTE": '{repr(pre_execute)}',
                    }},
                    check=True,
                )
            except subprocess.CalledProcessError:
                log = Path("/root/.nextflow.log").read_text()
                print("\n\n\n\n\n" + log)
                raise
            """,
            0,
        )

        return code_block

    def get_fn_interface(self, nf_script_path_in_container: Path):
        input_name, input_t = list(self._python_inputs.items())[0]
        output_t = list(self._python_outputs.values())[0]

        code_block = dedent(f"""\
            def _read_resources() -> Dict:
                try:
                    with open(".latch/resources.json") as f:
                        return json.load(f)
                except FileNotFoundError:
                    return {{}}

            def allocate_cpu({input_name}: {type_repr(input_t)}) -> int:
                res = _read_resources()
                return max(1, res['cpu_cores']) if res.get('cpu_cores') is not None else 16

            def allocate_memory({input_name}: {type_repr(input_t)}) -> int:
                res = _read_resources()
                return max(1, res['memory_bytes'] // 1024**3) if res.get('memory_bytes') is not None else 48

            def allocate_disk({input_name}: {type_repr(input_t)}) -> int:
                res = _read_resources()
                return max(1, res['disk_bytes'] // 1024**3) if res.get('disk_bytes') is not None else 500

            def get_resources({input_name}: {type_repr(input_t)}):
        """)

        code_block += reindent(
            self._get_nextflow_fn_code(nf_script_path_in_container, pre_execute=True), 1
        )

        code_block += dedent(f"""\

            @custom_task(cpu=allocate_cpu, memory=allocate_memory, storage_gib=allocate_disk, pre_execute=get_resources, cache=True)
            def {self.name}(
                {input_name}: {type_repr(input_t)}
            ) -> {type_repr(output_t)}:
            """)

        return code_block

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
                    {field.name}=out_channels.get(f"{field.name}")
                    """,
                    2,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                    return {res_type.__name__}(
                ||return|str||
                    )
            """,
            0,
        ).replace("||return|str||", return_str)

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
                    {field.name}=out_channels.get(f"{field.name}")
                    """,
                    2,
                ).rstrip()
            )

        return_str = ",\n".join(results)

        return reindent(
            rf"""
                    return {res_type.__name__}(
                ||return|str||
                    )
            """,
            0,
        ).replace("||return|str||", return_str)

    def get_fn_code(self, nf_script_path_in_container: Path):
        code_block = dedent(self.get_fn_interface(nf_script_path_in_container))

        code_block += reindent(
            self._get_nextflow_fn_code(nf_script_path_in_container),
            1,
        )

        code_block += reindent(
            rf"""

            out_channels = {{}}
            files = [Path(f) for f in glob.glob(".latch/task-outputs/*.json")]

            for file in files:
                out_channels[file.stem] = json.loads(file.read_text())

            print(out_channels)

            upload_files(out_channels, LatchDir({repr(self.wf.output_directory.remote_path)}))

            out_channels = {{k: json.dumps(v) for k, v in out_channels.items()}}

            """,
            1,
        )

        code_block += self.get_fn_return_stmt()
        return code_block
