# Snakemake Integration

## Getting Started

Latch's snakemake integration allows developers to build graphical interfaces to expose their workflows to wet lab teams. It also provides managed cloud infrastructure for execution of the workflow's jobs.

A primary design goal for integration is to allow developers to register existing projects with minimal added boilerplate and modifications to code. Here we outline exactly what these changes are and why they are needed.

Recall a snakemake project consists of a `Snakefile` , which describes workflow
rules in an ["extension"](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html) of Python, and associated python code imported and called by these rules. To make this project compatible with Latch, we need to do the following:

1. Identify and construct explicit parameters for each file dependency in `latch_metadata.py`
2. Build a container with all runtime dependencies
3. Ensure your `Snakefile` is compatible with cloud execution

### Step 1: Construct a `latch_metadata.py` file

The snakemake framework was designed to allow developers to both define and execute their workflows. This often means that the workflow parameters are sometimes ill-defined and scattered throughout the project as configuration values, static values in the `Snakefile` or command line flags.

To construct a graphical interface from a snakemake workflow, the file parameters need to be explicitly identified and defined so that they can be presented to scientist to be filled out through a web application. The `latch_metadata.py` file holds these parameter definitions, along with any styling or cosmetic modifications the developer wishes to make to each parameter.

*Currently, only file and directory parameters are supported*.

To identify the file "dependencies" that should be pulled out as parameters, it
can be useful to start with the `config.yaml` file that is used to configure
many Snakemake projects. Thinking about the minimum set of files needed to run
a successful workflow on fresh machine can also help identify these parameters.

Below is an example of how to create the `latch_metadata.py` file based on the `config.yaml` file:

Example of `config.yaml` file:

```yaml
# config.yaml
r1_fastq: "tests/r1.fq.gz"
r2_fastq: "tests/r2.fq.gz"
path: "tests/hs38DH"
```

Example of `latch_metadata.py` file:

```python
# latch_metadata.py

from pathlib import Path

from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.types.metadata import LatchAuthor, SnakemakeFileParameter, SnakemakeMetadata

SnakemakeMetadata(
    display_name="fgbio Best Practise FASTQ -> Consensus Pipeline",
    author=LatchAuthor(
        name="Fulcrum Genomics",
    ),
    parameters={
        "r1_fastq": SnakemakeFileParameter(
            display_name="Read 1 FastQ",
            type=LatchFile,
            path=Path("tests/r1.fq.gz"),
        ),
        "r2_fastq": SnakemakeFileParameter(
            display_name="Read 2 FastQ",
            type=LatchFile,
            path=Path("tests/r2.fq.gz"),
        ),
        "genome": SnakemakeFileParameter(
            display_name="Reference Genome",
            type=LatchDir,
            path=Path("tests/hs38DH"),
        ),
    },
)
```

### Step 2: Define all dependencies in a container

When executing Snakemake jobs on Latch, the jobs run within an environment specified by a `Dockerfile` . It is important to ensure that all required dependencies, whether they are third-party binaries, python libraries, or shell scripts, are correctly installed and configured within this `Dockerfile` so the job has access to them.

**Key Dependencies to Consider**:
* Python Packages:
  + Specify these in a `requirements.txt` or `environment.yaml` file.
* Conda Packages:
  + List these in an `environment.yaml` file.
* Bioinformatics Tools:
  + Often includes third-party binaries. They will need to be manually added to the Dockerfile.
* Snakemake wrappers and containers:
  + Note that while many Snakefile rules use singularity or docker containers, Latch doesn't currently support these wrapper or containerized environments. Therefore, all installation codes for these must be manually added into the Dockerfile.

**Generating a Customizable Dockerfile:**

To generate a `Dockerfile` that can be modified, use the following command:

 `latch dockerfile <workflow_folder>`

The above command searches for the `environment.yaml` and `requirements.txt` files within your project directory. Based on these, it generates Dockerfile instructions to install the specified Conda and Python dependencies.

Once the Dockerfile is generated, you can manually append it with third-party Linux installations or source codes related to Snakemake wrappers or containers.

When you register your snakemake project with Latch, a container is automatically built from the generated Dockerfile.

### Step 3: Ensure your `Snakefile` is compatible with cloud execution

When snakemake workflows are executed on Latch, each generated job is run in a separate container on a potentially isolated machine. This means your `Snakefile` might need to be modified to address problems that arise from this type of execution that were not present when executing locally:

* Add missing rule inputs that are implicitly fulfiled when executing locally. Index files for biological data are commonly expected to always be alongside their matching data.
* Make sure shared code does not rely on input files. This is any code that is not under a rule and so gets executed by every task
* Add `resources` directives if tasks run out of memory or disk space
* Optimize data transfer by merging tasks that have 1-to-1 dependencies

### Step 4: Register your project

When the above steps have been taken, it is safe to register your project with the Latch CLI.

Example: `latch register <workflow_folder>/ --snakefile <workflow_folder>/Snakefile`

This command will build a container and construct a graphical interface from your `latch_metdata.py` file. When this process has completed, a link to view your workflow on the Latch console will be printed to `stdout` .

---

## Lifecycle of a Snakemake Execution on Latch

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

## Metadata

Workflow metadata is read from the Snakefile. For this purpose, `SnakemakeMetadata` should be instantiated at the beginning of the file outside of any rules.

### Dependency Issues

Some Snakefiles import third-party dependencies at the beginning. This will cause the metadata extraction to fail if the dependencies are not installed. There are two ways of dealing with this problem:

1. Install the missing dependencies on the registering computer (the computer running the `latch` command)
2. Use a `latch_metadata.py` file

If registration fails before metadata can be pulled, the CLI will generate an example `latch_metadata.py` file.

### Input Parameters

Since there is no explicit entrypoint ( `@workflow` ) function in a Snakemake workflow, parameters are instead specified in the metadata file.

Currently only `LatchFile` and `LatchDir` parameters are supported. Both directory and file inputs are specified using `SnakemakeFileParameter` and setting the `type` field as appropriate.

Parameters must include a `path` field which specifies where the data will be downloaded to. This usually matches some file location expected by a Snakemake rule. Frequently, instead of simple paths, a rule with use a `configfile` to dynamically find input paths. In this case the only requiremtn is that the path matches the config file included in the workflow Docker image.

Example:

```py
parameters = {
  "example": SnakemakeFileParameter(
    display_name="Example Parameter",
    type=LatchFile,
    path=Path("example.txt"),
  )
}
```

## Troubleshooting

| Problem                                                                                                                                                  | Common Solution                                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `The above error occured when reading the Snakefile to extract workflow metadata.` | Snakefile has errors outside of any rules. Frequently caused by missing dependencies (look for `ModuleNotFoundError` ). Either install dependencies or add a `latch_metadata.py` file |
| `snakemake.exceptions.WorkflowError: Workflow defines configfile config.yaml but it is not present or accessible (full checked path: /root/config.yaml)` | Include a `config.yaml` in the workflow Docker image. Currently, config files cannot be generated from workflow parameters.                                                          |
| `Command '['/usr/local/bin/python', '-m', 'latch_cli.snakemake.single_task_snakemake', ...]' returned non-zero exit status 1.` | The runtime single-job task failed. Look at logs to find the error. It will be marked with the string `[!] Failed` .                                                                  |
| Runtime workflow task fails with `FileNotFoundError in file /root/workflow/Snakefile` but the file is specified in workflow parameters                   | Wrap the code that reads the file in a function. **See section "Input Files Referenced Outside of Rules"**                                                                           |
| MultiQC `No analysis results found. Cleaning up..` | FastQC outputs two files for every FastQ file: the raw `.zip` data and the HTML report. Include the raw `.zip` outputs of FastQC in the MultiQC rule inputs. **See section "Input Files Not Explicitly Defined in Rules"** "

## Troubleshooting: Input Files Referenced Outside of Rules

Only the JIT workflow downloads every input file. Tasks at runtime will only download files their target rules explicitly depend on. This means that Snakefile code that is not under a rule will usually fail if it tries to read input files.

**Example:**

```python
# ERROR: this reads a directory, regardless of which rule is executing!
samples = Path("inputs").glob("*.fastq")

rule all:
  input:
    expand("fastqc/{sample}.html", sample=samples)

rule fastqc:
  input:
    "inputs/{sample}.fastq"
  output:
    "fastqc/{sample}.html"
  shellcmd:
    fastqc {input} -o {output}
```

Since the `Path("inputs").glob(...)` call is not under any rule, _it runs in all tasks._ Because the `fastqc` rule does not specify `input_dir` as an `input` , it will not be downloaded and the code will throw an error.

### Solution

Only access files when necessary (i.e. when computing dependencies as in the example, or in a rule body) by placing problematic code within rule definitions. Either directly inline the variable or write a function to use in place of the variable.

**Example:**

```python
rule all_inline:
  input:
    # This code will only run in the JIT step
    expand("fastqc/{sample}.html", sample=Path("inputs").glob("*.fastq"))

def get_samples():
  # This code will only run if the function is called
  samples = Path("inputs").glob("*.fastq")
  return samples

rule all_function:
  input:
    expand("fastqc/{sample}.html", sample=get_samples())
```

This works because the JIT step replaces `input` , `output` , `params` , and other declarations with static strings for the runtime workflow so any function calls within them will be replaced with pre-computed strings and the Snakefile will not attempt to read the files again.

**Same example at runtime:**

```python
rule all_inline:
  input:
    "fastqc/example.html"

def get_samples():
  # Note: this function is no longer called anywhere in the file
  samples = Path("inputs").glob("*.fastq")
  return samples

rule all_function:
  input:
    "fastqc/example.html"
```

**Example using multiple return values:**

```python
def get_samples_data():
  samples = Path("inputs").glob("*.fastq")
  return {
    "samples": samples,
    "names": [x.name for x in samples]
  }

rule all:
  input:
    expand("fastqc/{sample}.html", sample=get_samples_data()["samples"]),
    expand("reports/{name}.txt", name=get_samples_data()["names"]),
```

## Troubleshooting: Input Files Not Explicitly Defined in Rules

When running the snakemake workflow locally, not all input files must be explicitly defined in every rule because all files are generated on one computer. However, tasks on Latch only download files specified by their target rules. Thus, unspecified input files will cause the Snakefile rule to fail due to missing input files.

**Example**

```python
# ERROR: the .zip file produced by the the fastqc rule is not found in the multiqc rule!

WORKDIR = "/root/"

rule fastqc:
  input: join(WORKDIR, 'fastq', 'raw', "{sample}.fastq")
  output:
      html = join(WORKDIR, "QC", "fastqc", 'raw', "Sample_{sample}")
  params:
      join(WORKDIR, "QC","fastqc", 'raw', "Sample_{sample}")
  run:
      if not os.path.exists(join(WORKDIR, str(params))):
          os.makedirs(join(WORKDIR, str(params)))
      shell("fastqc -o {params} --noextract -k 5 -t 8 -f fastq {input} 2>{log}")

rule multiqc:
    input:
      aligned_sequences = join(WORKDIR, "plasmid_wells_aligned_sequences.csv")
    output: directory(join(WORKDIR, "QC", "multiqc_report", 'raw'))
    params:
        join(WORKDIR, "QC", "fastqc", 'raw')
    benchmark:
        join(BENCHMARKDIR, "multiqc.txt")
    log:
        join(LOGDIR, "multiqc.log")
    shell:
        "multiqc {params} -o {output} --force"
```

### Solution

For programs that produce multiple types of input files (e.g. `.zip` and `.html` in the case of FastQC), explicitly specify these files in the outputs of the previous rule and in the inputs of the subsequent rule.

**Example**

```python
def get_samples():
  samples = Path("/root").glob("*fastqc.zip")
  return samples

WORKDIR = "/root/"
rule fastqc:
  input: join(WORKDIR, 'fastq', 'raw', "{sample}.fastq")
  output:
      html = join(WORKDIR, "QC", "fastqc", 'raw', "Sample_{sample}", "_{sample}_fastqc.html")
      # Specify zip as the output for every sample from fastqc
      zip = join(WORKDIR, "QC", "fastqc", 'raw', "Sample_{sample}", "_{sample}_fastqc.zip")
  params:
      join(WORKDIR, "QC","fastqc", 'raw', "Sample_{sample}")
  run:
      if not os.path.exists(join(WORKDIR, str(params))):
          os.makedirs(join(WORKDIR, str(params)))
      shell("fastqc -o {params} --noextract -k 5 -t 8 -f fastq {input} 2>{log}")

rule multiqc:
    input:
      aligned_sequences = join(WORKDIR, "plasmid_wells_aligned_sequences.csv")
      # Specify zip as the input for every sample from fastqc
      zip = expand(
            join(WORKDIR, "QC", "fastqc", 'raw', "Sample_{sample}", "_{sample}_fastqc.zip"), sample=get_samples()
        )
    output: directory(join(WORKDIR, "QC", "multiqc_report", 'raw'))
    params:
        join(WORKDIR, "QC", "fastqc", 'raw')
    benchmark:
        join(BENCHMARKDIR, "multiqc.txt")
    log:
        join(LOGDIR, "multiqc.log")
    shell:
       # Explicitly pass the input into the script instead of the Snakefile rule `params`
       # Before: "multiqc {params} -o {output} --force"
       # After
       "multiqc {input.zip} -o {output} --force"
```

## Snakemake Roadmap

### Known Issues

* Task caching does not work, tasks always re-run when a new version of the workflow is run even if nothing specific has changed
* It is not possible to configure the amount of available ephemeral storage
* Remote registration is not supported
* Snakemake tasks are serialized using a faulty custom implementation which does not support things like caching. Should use actual generated python code instead
* JIT workflow image should run snakemake extraction as a smoketest before being registered as a workflow
* Workflows with no parameters break the workflow params page on console UI
* Cannot set parameter defaults
* Parameter keys are unusued but are required in the metadata
* Log file tailing does not work

### Future Work

* Warn when the Snakefile reads files not on the docker image outside of any rules
* FUSE
* File/directory APIs
