# Troubleshooting

The following page outlines common problems with uploading Snakemake workflows and solutions.

| Problem                                                                                                                                                  | Common Solution                                                                                                                                                                      |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `The above error occured when reading the Snakefile to extract workflow metadata.` | Snakefile has errors outside of any rules. Frequently caused by missing dependencies (look for `ModuleNotFoundError` ). Either install dependencies or add a `latch_metadata.py` file |
| `snakemake.exceptions.WorkflowError: Workflow defines configfile config.yaml but it is not present or accessible (full checked path: /root/config.yaml)` | Include a `config.yaml` in the workflow Docker image. Currently, config files cannot be generated from workflow parameters.                                                          |
| `Command '['/usr/local/bin/python', '-m', 'latch_cli.extras.snakemake.single_task_snakemake', ...]' returned non-zero exit status 1.` | The runtime single-job task failed. Look at logs to find the error. It will be marked with the string `[!] Failed` .                                                                  |
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
