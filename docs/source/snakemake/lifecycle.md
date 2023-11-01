# Lifecycle of a Snakemake Execution on Latch

Snakemake support is currently based on JIT (Just-In-Time) registraton. This means that the workflow produced by `latch register` will only register a second workflow, which will run the actual pipeline tasks. This is because the actual structure of the workflow cannot be specified until parameter values are provided.

### JIT Workflow

The first ("JIT") workflow does the following:

1. Download all input files
2. Import the Snakefile, calculate the dependency graph, determine which jobs need to be run
3. Generate a Latch SDK workflow Python script for the second ("runtime") workflow and register it
4. Run the runtime workflow using the same inputs

Debugging:

* The generated runtime workflow entrypoint is uploaded to `latch:///.snakemake_latch/workflows/<workflow_name>/entrypoint.py`
* Internal workflow specifications are uploaded to `latch:///.snakemake_latch/workflows/<workflow_name>/spec`

### Runtime Workflow

The runtime workflow contains a task per each Snakemake job. This means that there will be a separate task per each wildcard instatiation of each rule. This can lead to workflows with hundreds of tasks. Note that the execution graph can be filtered by task status.

Each task runs a modified Snakemake executable using a script from the Latch SDK which monkey-patches the appropriate parts of the Snakemake package. This executable is different in two ways:

1. Rules that are not part of the task's target are entirely ignored
2. The target rule has all of its properties (currently inputs, outputs, benchmark, log, shellcode) replaced with the job-specific strings. This is the same as the value of these directives with all wildcards expanded and lazy values evaluated

Debugging:

* The Snakemake-compiled tasks are uploaded to `latch:///.snakemake_latch/workflows/<workflow_name>/compiled_tasks`

#### Example

Snakefile rules:

```Snakemake
rule all:
  input:
    os.path.join(WORKDIR, "qc", "fastqc", "read1_fastqc.html"),
    os.path.join(WORKDIR, "qc", "fastqc", "read2_fastqc.html")

rule fastqc:
  input: os.path.join(WORKDIR, "fastq", "{sample}.fastq")
  output: os.path.join(WORKDIR, "qc", "fastqc", "{sample}_fastqc.html")
  shellcmd: "fastqc {input} -o {output}"
```

Produced jobs:

1. Rule: `fastqc` Wildcards: `sample=read1`
1. Rule: `fastqc` Wildcards: `sample=read2`

Resulting single-job executable for job 1:

```py
# @workflow.rule(name='all', lineno=1, snakefile='/root/Snakefile')
# @workflow.input( # os.path.join(WORKDIR, "qc", "fastqc", "read1_fastqc.html"),
#     # os.path.join(WORKDIR, "qc", "fastqc", "read2_fastqc.html"),
# )
# @workflow.norun()
# @workflow.run
# def __rule_all(input, output, ...):
#     pass

@workflow.rule(name='fastqc', lineno=6, snakefile='/root/Snakefile')
@workflow.input("work/fastq/read1.fastq" # os.path.join(WORKDIR, "fastq", "{sample}.fastq")
)
@workflow.shellcmd("fastqc work/fastq/read1.fastq -o work/qc/fastqc/read1_fastqc.html")
@workflow.run
def __rule_fastqc(input, output, ...):
    shell("fastqc {input} -o {output}", ...)
```

Note:

* The "all" rule is entirely commented out
* The "fastqc" rule has no wildcards in its decorators

### Limitations

1. The workflow will execute the first rule defined in the Snakefile (matching standard Snakemake behavior). There is no way to change the default rule other than by moving the desired rule up in the file
1. The workflow will output files that are not used by downstream tasks. This means that intermediate files cannot be included in the output. The only way to exclude an output is to write a rule that lists it as an input
1. Input files and directories are downloaded fully, even if they are not used to generate the dependency graph. This commonly leads to issues with large directories being downloaded just to list the files contained within, delaying the JIT workflow by a large amount of time and requiring a large amount of disk space
1. Only the JIT workflow downloads input files. Rules only download their individual inputs, which can be a subset of the input files. If the Snakefile tries to read input files outside of rules it will usually fail at runtime
1. Large files that move between tasks will need to be uploaded by the outputting task and downloaded by each consuming task. This can take a large amount of time. Frequently it's possible to merge the producer and the consumer into one task to improve performance
1. Environment dependencies (Conda packages, Python packages, other software) must be well-specified. Missing dependencies will lead to JIT-time or runtime crashes
1. Config files are not supported and must be hard-coded into the workflow Docker image
1. `conda` directives will frequently fail with timeouts/SSL errors because Conda does not react well to dozens of tasks trying to install conda environments over a short timespan. It is recommended that all conda environments are included in the Docker image
1. The JIT workflow hard-codes the latch paths for rule inputs, outputs and other files. If these files are missing when the runtime workflow task runs, it will fail
