import json
from pathlib import Path
from typing import Annotated, Dict, List, Mapping, Type, get_args, get_origin

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.types.metadata import ParameterType

from ...common.utils import is_blob_type, is_samplesheet_param, reindent, type_repr
from ..workflow import NextflowWorkflow
from .base import NextflowBaseTask, NFTaskType


class NextflowOperatorTask(NextflowBaseTask):
    def __init__(
        self,
        inputs: Mapping[str, Type[ParameterType]],
        outputs: Mapping[str, Type[ParameterType]],
        id: str,
        name: str,
        statement: str,
        ret: List[str],
        branches: Dict[str, bool],
        script_path: Path,
        calling_subwf_name: str,
        wf: NextflowWorkflow,
    ):
        self.statement = statement
        self.ret = ret
        self.script_path = script_path
        self.calling_subwf_name = calling_subwf_name

        super().__init__(inputs, outputs, id, name, branches, wf, NFTaskType.Operator)

    def get_fn_interface(self):
        res = ""

        outputs_str = "None:"
        if len(self._python_outputs.items()) > 0:
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

            outputs_str = f"Res{self.name}:"

        params_str = ",\n".join(
            reindent(
                rf"""
                {param}: {type_repr(t)}
                """,
                1,
            ).rstrip()
            for param, t in self._python_inputs.items()
        )

        res += (
            reindent(
                rf"""
                @task(cache=True)
                def {self.name}(
                ||params||
                ) -> ||outputs||
                """,
                0,
            )
            .replace("||params||", params_str)
            .replace("||outputs||", outputs_str)
        )

        return res

    def get_fn_return_stmt(self):
        results: List[str] = []
        for out_name in self._python_outputs.keys():
            results.append(
                reindent(
                    rf"""
                    {out_name}=out_channels.get("{out_name}")
                    """,
                    2,
                ).rstrip()
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

        run_task_entrypoint = [
            "/root/nextflow",
            "run",
            str(nf_script_path_in_container),
            "-lib",
            "lib",
        ]

        for flag, val in self.wf.flags_to_params.items():
            run_task_entrypoint.extend([flag, str(val)])

        code_block += reindent(
            """
            wf_paths = {}
            """,
            2,
        )

        for k, typ in self.wf_inputs.items():
            if k[3:] in self.wf.downloadable_params:
                code_block += reindent(
                    f"""
                    if {k} is not None:
                        {k}_p = Path({k}).resolve()
                        check_exists_and_rename({k}_p, Path("/root") / {k}_p.name)
                        wf_paths["{k}"] = Path("/root") / {k}_p.name

                    """,
                    2,
                )
            elif is_blob_type(typ):
                code_block += reindent(
                    f"""
                    if {k} is not None:
                        {k}_p = Path("/root/").resolve() # superhack
                        wf_paths["{k}"] = {k}_p

                    """,
                    2,
                )
            elif is_samplesheet_param(typ):
                code_block += reindent(
                    f"""
                    {k} = construct_samplesheet({k})
                    """,
                    2,
                )

        # todo(ayush): figure out how to make this work
        do_file_io = False
        for op_name in [
            "collectFile",
            "countFasta",
            "countFastq",
            "countJson",
            "countLines",
            "splitCsv",
            "splitFasta",
            "splitFastq",
            "splitJson",
            "splitText",
            "fromPath",
        ]:
            if op_name in self.name:
                do_file_io = True
                break

        upload_str = ""
        download_str = ""
        if do_file_io:
            download_str = rf"""download_files(channel_vals, LatchDir({repr(self.wf.output_directory.remote_path)}))"""
            upload_str = rf"""upload_files(out_channels, LatchDir({repr(self.wf.output_directory.remote_path)}))"""

        if self.script_path.resolve() != self.wf.nf_script.resolve():
            stem = self.script_path.resolve().relative_to(self.wf.pkg_root.resolve())
            run_task_entrypoint[2] = str(Path("/root") / stem)
            run_task_entrypoint.extend(["-entry", self.calling_subwf_name])

        code_block += reindent(
            rf"""
                    channel_vals = [{", ".join([f"json.loads({x})" for x in self.channel_inputs])}]

                    {download_str}

                    try:
                        subprocess.run(
                            [{', '.join([f"str({x})" if x.startswith("wf_") else repr(x) for x in run_task_entrypoint])}],
                            env={{
                                **os.environ,
                                "LATCH_CONFIG_DIR_OVERRIDE": str(Path.cwd()),
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
                        out_channels[file.stem] = json.loads(file.read_text())

                    {upload_str}

                    out_channels = {{k: json.dumps(v) for k, v in out_channels.items()}}

                else:
                    print("TASK SKIPPED")
                    try:
                        _override_task_status(status="SKIPPED")
                    except Exception as e:
                        print(f"Failed to override task status: {{e}}")
                    out_channels = {{||skip||}}

            """,
            1,
        ).replace(
            "||skip||",
            ", ".join([f"{repr(o)}: None" for o in self._python_outputs.keys()]),
        )

        code_block += self.get_fn_return_stmt()
        return code_block
