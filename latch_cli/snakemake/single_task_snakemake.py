import json
import os
import sys
import traceback
from itertools import chain
from textwrap import dedent
from typing import Union

import snakemake
from snakemake.io import AnnotatedString
from snakemake.parser import (
    INDENT,
    Benchmark,
    Input,
    Log,
    Output,
    Params,
    Rule,
    StopAutomaton,
    is_comment,
    is_dedent,
    is_indent,
    is_newline,
)

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

    if isinstance(self, Input):
        xs = cur_data["inputs"]
    elif isinstance(self, Output):
        xs = cur_data["outputs"]
    elif isinstance(self, Params):
        xs = cur_data["params"]
    elif isinstance(self, Benchmark):
        xs = {"positional": cur_data["benchmark"]}
    elif isinstance(self, Log):
        xs = {"positional": cur_data["log"]}
    else:
        raise ValueError(f"tried to emit overrides for unknown state: {type(self)}")

    positional_data = (render_annotated_str_list(x) for x in xs["positional"])
    keyword_data = (
        f"{k}={render_annotated_str_list(v)}" for k, v in xs["keyword"].items()
    )
    data = chain(positional_data, keyword_data)

    for x in data:
        yield x, token
        yield ",", token
        yield "\n", token

        # we'll need to re-indent the commented-out originals too
        yield INDENT * self.base_indent, token


emitted_overrides_per_type: dict[str, set[str]] = {}


# Skip @workflow.input and @workflow.output for non-target tasks
def skip_block(self, token, force_block_end=False):
    if self.lasttoken == "\n" and is_comment(token):
        # ignore lines containing only comments
        self.line -= 1
    if force_block_end or self.is_block_end(token):
        yield from self.decorate_end(token)
        yield "\n", token
        raise StopAutomaton(token)

    if is_newline(token):
        self.line += 1
        yield token.string, token

    elif not (is_indent(token) or is_dedent(token)):
        if is_comment(token):
            yield token.string, token
        else:
            try:
                at_newline = self.lasttoken == "\n"

                # old snakemake sometime does not put a newline after the decorate parenthesis
                at_start = self.line == 0 and self.lasttoken[-1] == "("

                if at_newline or at_start:
                    emitted_overrides = emitted_overrides_per_type.setdefault(
                        type(self).__name__, set()
                    )

                    if (
                        self.rulename in rules
                        and self.rulename not in emitted_overrides
                    ):
                        yield from emit_overrides(self, token)
                        emitted_overrides.add(self.rulename)

                    yield "#", token
            except:
                traceback.print_exc()

            yield from self.block_content(token)


Input.block = skip_block
Output.block = skip_block
Params.block = skip_block
Benchmark.block = skip_block
Log.block = skip_block

# Run snakemake
snakemake.main()
