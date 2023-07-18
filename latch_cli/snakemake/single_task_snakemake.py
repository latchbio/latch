import json
import os
import sys
import traceback
from itertools import chain
from textwrap import dedent

import snakemake
from snakemake.parser import (
    INDENT,
    Input,
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

eprint("\n>>><<<\n")
eprint("Using LATCH_SNAKEMAKE_DATA:")
for rule in rules:
    rule_data = rules[rule]
    eprint(f"  {rule}:")

    eprint("    Inputs:")
    for x in rule_data["inputs"]:
        eprint(f"      {repr(x)}")

    eprint("    Outputs:")
    for x in rule_data["outputs"]:
        eprint(f"      {repr(x)}")

    eprint("    Nameless Params:")
    for x in rule_data["nameless_params"]:
        eprint(f"      {repr(x)}")

    eprint("    Params:")
    for k, v in rule_data["params"].items():
        eprint(f"      {k}={repr(v)}")

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
            {outputs_str}
            )
            @workflow.norun()
            @workflow.run
            def __rule_latch_entrypoint(input, output, params, wildcards, threads, resources, log, version, rule, conda_env, container_img, singularity_args, use_singularity, env_modules, bench_record, jobid, is_shell, bench_iteration, cleanup_scripts, shadow_dir, edit_notebook, conda_base_path, basedir, runtime_sourcecache_path, __is_snakemake_rule_func=True):
                pass

            """)

    yield prefix + next(_real_rule_start(self, aux))


Rule.start = rule_start


def emit_overrides(self, token):
    cur_data = rules[self.rulename]

    if isinstance(self, Input):
        xs = (repr(x) for x in cur_data["inputs"])
    elif isinstance(self, Output):
        xs = (repr(x) for x in cur_data["outputs"])
    elif isinstance(self, Params):
        nameless_params = cur_data["nameless_params"]
        n_params_gen = (f"{repr(x)}" for x in nameless_params)

        params = cur_data["params"]
        params_gen = (f"{k}={repr(v)}" for k, v in params.items())

        xs = chain(n_params_gen, params_gen)
    else:
        raise ValueError(f"tried to emit overrides for unknown state: {type(self)}")

    for x in xs:
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

# Run snakemake
snakemake.main()
