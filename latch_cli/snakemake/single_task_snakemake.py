import json
import os
from textwrap import dedent

import snakemake
from snakemake.parser import (
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

data = json.loads(os.environ["LATCH_SNAKEMAKE_DATA"])
rules = data["rules"]
outputs = data["outputs"]

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
            if (
                self.lasttoken == "\n"
                or
                # old snakemake sometime does not put a newline after the decorate parenthesis
                (self.line == 0 and self.lasttoken[-1] == "(")
            ):
                if self.rulename in rules:
                    cur_data = rules[self.rulename]
                    if isinstance(self, Params):
                        xs = cur_data["params"]

                        for k, v in xs.items():
                            yield f"{k}={repr(v)}", token

                    else:
                        xs = []
                        if isinstance(self, Input):
                            xs = cur_data["inputs"]
                        if isinstance(self, Output):
                            xs = cur_data["outputs"]

                        for x in xs:
                            yield repr(x), token

                yield "#", token

            yield from self.block_content(token)


Input.block = skip_block
Output.block = skip_block
Params.block = skip_block

# Run snakemake
snakemake.main()
