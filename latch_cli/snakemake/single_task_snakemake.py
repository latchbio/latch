import json
import os
import sys
from itertools import chain
from textwrap import dedent
from typing import Dict, Set

import snakemake
from snakemake.parser import (
    INDENT,
    Benchmark,
    Input,
    Log,
    Output,
    Params,
    Python,
    Rule,
    Shell,
)
from snakemake.rules import Rule as RRule

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)


def eprint(x: str) -> None:
    print(x, file=sys.stderr)


data = json.loads(os.environ["LATCH_SNAKEMAKE_DATA"])
rules = data["rules"]
outputs = data["outputs"]


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
    if flags.get("directory", False):
        res = f"directory({res})"
        del flags["directory"]

    if len(flags) != 0:
        raise RuntimeError(f"found unsupported flags: {repr(flags)}")

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

    positional_data = (render_annotated_str_list(x) for x in xs["positional"])
    keyword_data = (
        f"{k}={render_annotated_str_list(v)}" for k, v in xs["keyword"].items()
    )
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


Input.block_content = skipping_block_content
Output.block_content = skipping_block_content
Params.block_content = skipping_block_content
Benchmark.block_content = skipping_block_content
Log.block_content = skipping_block_content


class SkippingRule(Rule):
    def start(self, aux=""):
        if self.rulename not in rules:
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

# Run snakemake
snakemake.main()
