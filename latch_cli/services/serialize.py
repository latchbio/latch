import os
import textwrap
from itertools import chain, filterfalse
from pathlib import Path

from snakemake.common import ON_WINDOWS
from snakemake.dag import DAG
from snakemake.persistence import Persistence
from snakemake.rules import Rule
from snakemake.workflow import Workflow


class SnakemakeWorkflowExtractor(Workflow):
    def __init__(self, snakefile):
        super().__init__(snakefile=snakefile)

    def extract(self):
        def rules(items):
            return map(self._rules.__getitem__, filter(self.is_rule, items))

        def files(items):
            relpath = (
                lambda f: f
                if os.path.isabs(f) or f.startswith("root://")
                else os.path.relpath(f)
            )
            return map(relpath, filterfalse(self.is_rule, items))

        # if not targets and not target_jobs:
        targets = [self.default_target] if self.default_target is not None else list()

        prioritytargets = list()
        forcerun = list()
        until = list()
        omit_from = list()

        priorityrules = set(rules(prioritytargets))
        priorityfiles = set(files(prioritytargets))
        forcerules = set(rules(forcerun))
        forcefiles = set(files(forcerun))
        untilrules = set(rules(until))
        untilfiles = set(files(until))
        omitrules = set(rules(omit_from))
        omitfiles = set(files(omit_from))
        targetrules = set(
            chain(
                rules(targets),
                filterfalse(Rule.has_wildcards, priorityrules),
                filterfalse(Rule.has_wildcards, forcerules),
                filterfalse(Rule.has_wildcards, untilrules),
            )
        )
        targetfiles = set(chain(files(targets), priorityfiles, forcefiles, untilfiles))

        if ON_WINDOWS:
            targetfiles = set(tf.replace(os.sep, os.altsep) for tf in targetfiles)

        rules = self.rules

        dag = DAG(
            self,
            rules,
            targetfiles=targetfiles,
            targetrules=targetrules,
            forcefiles=forcefiles,
            forcerules=forcerules,
            priorityfiles=priorityfiles,
            priorityrules=priorityrules,
            untilfiles=untilfiles,
            untilrules=untilrules,
            omitfiles=omitfiles,
            omitrules=omitrules,
        )

        self.persistence = Persistence(
            dag=dag,
        )

        dag.init()
        dag.update_checkpoint_dependencies()
        dag.check_dynamic()

        return self, dag


def serialize(pkg_root: Path):
    """Serializes workflow code into lyteidl protobuf.

    Args:
        pkg_root: The directory of project with workflow code to be serialized
    """

    ...


def generate_snakemake_entrypoint(snakefile: Path):

    workflow = SnakemakeWorkflowExtractor(
        snakefile=snakefile,
    )
    workflow.include(
        snakefile,
        overwrite_default_target=True,
    )
    wf, dag = workflow.extract()

    entrypoint_code_block = textwrap.dedent(
        """\
           import subprocess
           from pathlib import Path
           from typing import NamedTuple

           from latch import small_task
           from latch.types import LatchFile

           def ensure_parents_exist(path: Path):
               path.parent.mkdir(parents=True, exist_ok=True)
               return path
           """
    )

    with open("latch_entrypoint.py", "w") as f:
        f.write(entrypoint_code_block)


def generate_python_function_for_job():

    code_block = ""

    fn_interface = f"\n\n@small_task\ndef {self.name}("
    for idx, (param, t) in enumerate(self._python_inputs.items()):
        fn_interface += f"{param}: {t.__name__}"
        if idx == len(self._python_inputs) - 1:
            fn_interface += ")"
        else:
            fn_interface += ", "

    # NamedTuple("OP", B_bam=LatchFile)
    if len(self._python_outputs.items()) > 0:
        for idx, (param, t) in enumerate(self._python_outputs.items()):
            if idx == 0:
                fn_interface += f" -> NamedTuple('{self.name}_output', "
            fn_interface += f"{param}={t.__name__}"
            if idx == len(self._python_outputs) - 1:
                fn_interface += "):"
            else:
                fn_interface += ", "
    else:
        fn_interface += ":"

    code_block += fn_interface

    # rename statement

    for param, t in self._python_inputs.items():
        if t == LatchFile:
            code_block += f'\n\tPath({param}).resolve().rename(ensure_parents_exist(Path("{self._target_file_for_param[param]}")))'

    # Snakemake subprocess

    executor = RealExecutor(workflow, dag)
    executor.cores = 8
    snakemake_cmd = ["snakemake", *executor.get_job_args(self.job).split(" ")]
    snakemake_cmd.remove("")
    formatted_snakemake_cmd = "\n\n\tsubprocess.run(["

    for i, arg in enumerate(snakemake_cmd):
        arg_wo_quotes = arg.strip('"').strip("'")
        formatted_snakemake_cmd += f'"{arg_wo_quotes}"'
        if i == len(snakemake_cmd) - 1:
            formatted_snakemake_cmd += "], check=True)"
        else:
            formatted_snakemake_cmd += ", "
    code_block += formatted_snakemake_cmd

    return_stmt = "\n\treturn ("
    for i, x in enumerate(self._python_outputs):
        if self._is_target:
            return_stmt += (
                f"LatchFile('{self._target_file_for_param[x]}',"
                f" 'latch:///{self._target_file_for_param[x]}')"
            )
        else:
            return_stmt += f"LatchFile('{self._target_file_for_param[x]}')"
        if i == len(self._python_outputs) - 1:
            return_stmt += ")"
        else:
            return_stmt += ", "
    code_block += return_stmt
    return code_block
