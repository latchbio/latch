import json
import os
import shlex
import sys
from itertools import chain
from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional, Set
from urllib.parse import urlparse, urlunparse

import snakemake
import snakemake.workflow
from snakemake.deployment import singularity
from snakemake.parser import (
    INDENT,
    Benchmark,
    Include,
    Input,
    Log,
    Output,
    Params,
    Python,
    Rule,
    Ruleorder,
    Shell,
)
from snakemake.rules import Rule as RRule
from snakemake.workflow import Workflow as WWorkflow

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


def eprint(x: str) -> None:
    print(x, file=sys.stderr)


print_compilation = os.environ.get("LATCH_PRINT_COMPILATION", False)
if print_compilation == "1":
    print_compilation = True

data = json.loads(os.environ["LATCH_SNAKEMAKE_DATA"])
rules = data["rules"]
outputs = data["outputs"]

overwrite_config = data.get("overwrite_config", {})

old_workflow_init = WWorkflow.__init__


def new_init(self: WWorkflow, *args, **kwargs):
    kwargs["overwrite_config"] = overwrite_config
    old_workflow_init(self, *args, **kwargs)


WWorkflow.__init__ = new_init


def eprint_named_list(xs):
    eprint("      Positional:")
    for x in xs["positional"]:
        eprint(f"      {repr(x)}")

    eprint("      Keyword:")
    for k, v in xs["keyword"].items():
        eprint(f"      {k}={repr(v)}")


eprint("\n>>><<<\n")
eprint("Using LATCH_SNAKEMAKE_DATA:")
for rule in rules:
    rule_data = rules[rule]
    eprint(f"  {rule}:")

    eprint("    Inputs:")
    eprint_named_list(rule_data["inputs"])

    eprint("    Outputs:")
    eprint_named_list(rule_data["outputs"])

    eprint("    Params:")
    eprint_named_list(rule_data["params"])

    eprint("    Benchmark:")
    eprint(f"      {rule_data['benchmark']}")

    eprint("    Log:")
    eprint(f"      {rule_data['log']}")

    eprint("    Shellcmd:")
    eprint(f"      {rule_data['shellcmd']}")

eprint("\nExpected outputs:")
for x in outputs:
    eprint(repr(x))
eprint("\n>>><<<\n")

# Add a custom entrypoint rule
_real_rule_start = Rule.start


def rule_start(self, aux=""):
    prefix = ""
    if self.rulename in rules:
        outputs_str = ",\n".join(f"    {repr(x)}" for x in outputs)
        prefix = dedent(f"""
            @workflow.rule(name='latch_entrypoint', lineno=1, snakefile='workflow/Snakefile')
            @workflow.input(
            __outputs__
            )
            @workflow.norun()
            @workflow.run
            def __rule_latch_entrypoint(input, output, params, wildcards, threads, resources, log, version, rule, conda_env, container_img, singularity_args, use_singularity, env_modules, bench_record, jobid, is_shell, bench_iteration, cleanup_scripts, shadow_dir, edit_notebook, conda_base_path, basedir, runtime_sourcecache_path, __is_snakemake_rule_func=True):
                pass

            """).replace("__outputs__", outputs_str)

    yield prefix + next(_real_rule_start(self, aux))


Rule.start = rule_start


def render_annotated_str(x) -> str:
    if not isinstance(x, dict):
        return repr(x)

    value = x["value"]
    flags = dict(x["flags"])

    res = repr(value)

    if len(flags) > 1:
        raise RuntimeError(f"can only have one flag for {res} but found: {repr(flags)}")

    if "directory" in flags:
        res = f"directory({res})"

    elif "report" in flags:
        report_vals = flags.get("report", False)
        res = (
            f"report({res}, caption={repr(report_vals['caption'])},"
            f" category={report_vals['category']})"
        )

    elif "temp" in flags:
        # A temporary modifier is no different from a normal file as all files
        # are deleted on Latch after a job completes.
        # https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#protected-and-temporary-files
        del flags["temp"]

    return res


def render_annotated_str_list(xs) -> str:
    if not isinstance(xs, list):
        return render_annotated_str(xs)

    els = ", ".join(render_annotated_str(x) for x in xs)
    return f"[{els}]"


def emit_overrides(self, token):
    cur_data = rules[self.rulename]
    eprint(f"\nOverriding {self.rulename} {self.__class__.__name__}")

    if isinstance(self, Input):
        xs = cur_data["inputs"]
    elif isinstance(self, Output):
        xs = cur_data["outputs"]
    elif isinstance(self, Params):
        xs = cur_data["params"]
    elif isinstance(self, Benchmark):
        xs = {"positional": [cur_data["benchmark"]], "keyword": {}}
    elif isinstance(self, Log):
        xs = {"positional": [cur_data["log"]], "keyword": {}}
    else:
        raise ValueError(f"tried to emit overrides for unknown state: {type(self)}")

    if (
        isinstance(self, Output)
        and len(xs["positional"]) > 0
        and xs["positional"][0].get("flags") is not None
        and "multiext" in xs["positional"][0].get("flags")
    ):
        filename = repr(xs["positional"][0]["flags"]["multiext"])
        exts = [repr("." + x["value"].split(".")[-1]) for x in xs["positional"]]
        positional_data = (f"multiext({filename},{','.join(exts)})",)
    else:
        positional_data = (render_annotated_str_list(x) for x in xs["positional"])

    modifier_fn = render_annotated_str_list
    if isinstance(self, Params):
        modifier_fn = repr

    keyword_data = (f"{k}={modifier_fn(v)}" for k, v in xs["keyword"].items())
    data = chain(positional_data, keyword_data)

    for x in data:
        eprint(f"  {x}")

        yield x, token
        yield ",", token
        yield "\n", token

        # we'll need to re-indent the commented-out originals too
        yield INDENT * self.base_indent, token


emitted_overrides_per_type: Dict[str, Set[str]] = {}


def skipping_block_content(self, token):
    if self.rulename not in rules:
        return

    emitted_overrides = emitted_overrides_per_type.setdefault(
        type(self).__name__, set()
    )
    if self.rulename in emitted_overrides:
        return

    yield from emit_overrides(self, token)
    emitted_overrides.add(self.rulename)


def block_content_with_print_compilation(self, token):
    if print_compilation:
        yield f"{token.string}, print_compilation=True", token
    else:
        yield token.string, token


def empty_generator(self, token):
    return
    yield


Input.block_content = skipping_block_content
Output.block_content = skipping_block_content
Params.block_content = skipping_block_content
Benchmark.block_content = skipping_block_content
Log.block_content = skipping_block_content
Ruleorder.block_content = empty_generator
Include.block_content = block_content_with_print_compilation


class SkippingRule(Rule):
    def start(self, aux=""):
        if self.rulename not in rules:
            # Rules can be nested in conditional statements:
            #
            # if (<condition>):
            #   rule A:
            #       <stuff>
            #
            # We want correct python code if we remove them.
            yield "..."
            return

        yield from super().start(aux)

    def end(self):
        if self.rulename not in rules:
            return

        yield from super().end()

    def block_content(self, token):
        if self.rulename not in rules:
            return

        yield from super().block_content(token)


class SkippingCheckpoint(SkippingRule):
    def start(self):
        yield from super().start(aux=", checkpoint=True")


Python.subautomata["rule"] = SkippingRule
Python.subautomata["checkpoint"] = SkippingCheckpoint


class ReplacingShell(Shell):
    def __init__(self, snakefile, rulename, base_indent=0, dedent=0, root=True):
        if rulename in rules:
            cmd: str = rules[rulename]["shellcmd"]
            self.overwrite_cmd = cmd.replace("{", "{{").replace("}", "}}")

        super().__init__(snakefile, rulename, base_indent, dedent, root)


SkippingRule.subautomata["shell"] = ReplacingShell


def get_wildcards(self, requested_output, wildcards_dict=None):
    return wildcards_dict


RRule.get_wildcards = get_wildcards


def get_docker_cmd(
    img_path: str,
    cmd: str,
    args: str = "",
    quiet: bool = False,
    envvars: Optional[Dict[str, str]] = None,
    shell_executable: Optional[str] = None,
    container_workdir: Optional[str] = None,
    is_python_script: bool = False,
) -> str:
    if shell_executable is None:
        shell_executable = "sh"
    else:
        # Ensure to just use the name of the executable, not a path,
        # because we cannot be sure where it is located in the container.
        shell_executable = Path(shell_executable).name

    workdir = container_workdir if container_workdir is not None else "/root"
    if '"' in workdir:
        raise ValueError("container_workdir cannot contain double quotes")

    docker_cmd = ["docker", "run"]
    if envvars is not None:
        for k, v in envvars.items():
            docker_cmd.extend(["--env", f"{k}={v}"])

    if is_python_script:
        # mount host snakemake module into container
        docker_cmd.extend([
            "--mount",
            f'type=bind,"src={singularity.SNAKEMAKE_SEARCHPATH}","target={singularity.SNAKEMAKE_MOUNTPOINT}"',
        ])

    # support for containers currently only works with relative input and output
    # paths. This preserves the behavior before the monkey path
    docker_cmd.extend([
        "--workdir",
        workdir,
        "--mount",
        f'type=bind,src=.,"target={workdir}"',
    ])

    if args != "":
        docker_cmd.extend(shlex.split(args))

    docker_cmd.extend([
        img_path,
        shell_executable,
        "-c",
        cmd,
    ])

    return shlex.join(docker_cmd)


@property
def docker_path(self):
    parsed = urlparse(self.url)
    if parsed.scheme != "" and parsed.scheme != "docker":
        raise ValueError("Only docker images are supported")
    path = urlunparse(parsed._replace(scheme=""))
    return path[2:] if path.startswith("//") else path


# Monkey-patch snakemake to use docker container runtime instead of singularity
# because singularity does not work inside sysbox.
singularity.Image.pull = lambda self, dry_run: None
singularity.Image.path = docker_path
singularity.shellcmd = get_docker_cmd


# Run snakemake
snakemake.main()
