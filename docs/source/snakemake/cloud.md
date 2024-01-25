# Snakemake Workflow Cloud Compatibility

When Snakemake workflows are executed locally on a single computer or high-performance cluster, all dependencies and input/ output files are on a single machine.

When a Snakemake workflow is executed on Latch, each generated job is run in a separate container on a potentially isolated machine.

Therefore, it may be necessary to adapt your Snakefile to address issues arising from this execution method, which were not encountered during local execution:

- Add missing rule inputs that are implicitly fulfilled when executing locally.
- Make sure shared code does not rely on input files. This is any code that is not under a rule, and so gets executed by every task
- Add `resources` directives if tasks run out of memory or disk space
- Optimize data transfer by merging tasks that have 1-to-1 dependencies

Here, we will walk through examples of each of the cases outlined above.

## Add missing rule inputs

When a Snakemake workflow is executed on Latch, each generated job for the Snakefile rule is run on a separate machine. Only files and directories explicitly specified under the `input` directive of the rule are downloaded in the task.

A typical example is if the index files for biological data are not explicitly specified as a Snakefile input, the generated job for that rule will fail due to the missing index files.

#### Example

In the example below, there are two Snakefile rules:

- `delly_s`: The rule runs Delly to call SVs and outputs an unfiltered BCF file, followed by quality filtering using `bcftools` filter to retain only the SV calls that pass certain filters. Finally, it indexes the BCF file.
- `delly_merge`: This rule merges or concatenates BCF files containing SV calls from the delly_s rule, producing a single VCF file. The rule requires the index file to be available for each corresponding BAM file.

```python
rule delly_s:  # single-sample analysis
    input:
        fasta=get_fasta(),
        fai=get_faidx()[0],
        bam=get_bam("{path}/{sample}"),
        bai=get_bai("{path}/{sample}"),
        excl_opt=get_bed()
    params:
        excl_opt='-x "%s"' % get_bed() if exclude_regions() else "",
    output:
        bcf = os.path.join(
            "{path}",
            "{sample}",
            get_outdir("delly"),
            "delly-{}{}".format("{sv_type}", config.file_exts.bcf),
        )

    conda:
        "../envs/caller.yaml"
    threads: 1
    resources:
        mem_mb=config.callers.delly.memory,
        tmp_mb=config.callers.delly.tmpspace,
    shell:
        """
        set -xe

        OUTDIR="$(dirname "{output.bcf}")"
        PREFIX="$(basename "{output.bcf}" .bcf)"
        OUTFILE="${{OUTDIR}}/${{PREFIX}}.unfiltered.bcf"

        # run dummy or real job
        if [ "{config.echo_run}" -eq "1" ]; then
            echo "{input}" > "{output}"
        else
            # use OpenMP for threaded jobs
            export OMP_NUM_THREADS={threads}

            # SV calling
            delly call \
                -t "{wildcards.sv_type}" \
                -g "{input.fasta}" \
                -o "${{OUTFILE}}" \
                -q 1 `# min.paired-end mapping quality` \
                -s 9 `# insert size cutoff, DELs only` \
                {params.excl_opt} \
                "{input.bam}"
            # SV quality filtering
            bcftools filter \
                -O b `# compressed BCF format` \
                -o "{output.bcf}" \
                -i "FILTER == 'PASS'" \
                "${{OUTFILE}}"
            # index BCF file
            bcftools index "{output.bcf}"
        fi
        """

rule delly_merge:  # used by both modes
    input:
        bcf = [
            os.path.join(
                "{path}",
                "{tumor}--{normal}",
                get_outdir("delly"),
                "delly-{}{}".format(sv, config.file_exts.bcf),
            )
            for sv in config.callers.delly.sv_types
        ]
        if config.mode is config.mode.PAIRED_SAMPLE
        else [
            os.path.join(
                "{path}",
                "{sample}",
                get_outdir("delly"),
                "delly-{}{}".format(sv, config.file_exts.bcf),
            )
            for sv in config.callers.delly.sv_types
        ],
        if config.mode is config.mode.PAIRED_SAMPLE
        else [
            os.path.join(
                "{path}",
                "{sample}",
                get_outdir("delly"),
                "delly-{}{}".format(sv, config.file_exts.bcf),
            ) + ".csi"
            for sv in config.callers.delly.sv_types
        ]
    output:
        os.path.join(
            "{path}",
            "{tumor}--{normal}",
            get_outdir("delly"),
            "delly{}".format(config.file_exts.vcf),
        )
        if config.mode is config.mode.PAIRED_SAMPLE
        else os.path.join(
            "{path}",
            "{sample}",
            get_outdir("delly"),
            "delly{}".format(config.file_exts.vcf),
        ),
    conda:
        "../envs/caller.yaml"
    threads: 1
    resources:
        mem_mb=1024,
        tmp_mb=0,
    shell:
        """
         set -x

         # run dummy or real job
         if [ "{config.echo_run}" -eq "1" ]; then
             cat {input} > "{output}"
         else
             # concatenate rather than merge BCF files
             bcftools concat \
                -a `# allow overlaps` \
                -O v `# uncompressed VCF format` \
                -o "{output}" \
                {input.bcf}
        fi
        """
```

The above code will fail with the error:

```bash
Failed to open: /root/workflow/data/bam/3/T3--N3/delly_out/delly-BND.bcf.csi
```

### Solution

The task failed because the BAM index files (ending with `bcf.csi`) are produced by the `delly_s` rule but is not explicitly specified as input to the `delly_merge` rule. Hence, the index files are not downloaded into the task that executes the `delly_merge` rule.

To resolve the error, we need to add the index files as the output of the `delly_s` rule and the input of the `delly_merge` rule:

```python
rule delly_s:  # single-sample analysis
    input:
        fasta=get_fasta(),
        fai=get_faidx()[0],
        bam=get_bam("{path}/{sample}"),
        bai=get_bai("{path}/{sample}"),
        excl_opt=get_bed()
    params:
        excl_opt='-x "%s"' % get_bed() if exclude_regions() else "",
    output:
        bcf = os.path.join(
            "{path}",
            "{sample}",
            get_outdir("delly"),
            "delly-{}{}".format("{sv_type}", config.file_exts.bcf),
        ),

        # Add bcf_index as the rule's output
        bcf_index = os.path.join(
            "{path}",
            "{sample}",
            get_outdir("delly"),
            "delly-{}{}".format("{sv_type}", config.file_exts.bcf),
        ) + ".csi"
        ...
```

```python
rule delly_merge:  # used by both modes
    input:
        bcf = [
            os.path.join(
                "{path}",
                "{tumor}--{normal}",
                get_outdir("delly"),
                "delly-{}{}".format(sv, config.file_exts.bcf),
            )
            for sv in config.callers.delly.sv_types
        ]
        if config.mode is config.mode.PAIRED_SAMPLE
        else [
            os.path.join(
                "{path}",
                "{sample}",
                get_outdir("delly"),
                "delly-{}{}".format(sv, config.file_exts.bcf),
            )
            for sv in config.callers.delly.sv_types
        ],

        # Add bcf_index as input
        bcf_index = [
            os.path.join(
                "{path}",
                "{tumor}--{normal}",
                get_outdir("delly"),
                "delly-{}{}".format(sv, config.file_exts.bcf),
            ) + ".csi"
            for sv in config.callers.delly.sv_types
        ]
        ...
```

## Make sure shared code doesn't rely on input files

Tasks at runtime will only download files their target rules explicitly depend on. Shared code, or Snakefile code that is not under any rule, will usually fail if it tries to read input files.

#### Example

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

#### Example

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

## Add `resources` directives

It is common for a Snakefile rule to run into out-of-memory errors.

#### Example

The following workflow failed because Kraken2 requires at least 256GB of RAM to run.

```python
rule kraken:
    input:
        reads = lambda wildcards: get_samples()["sample_reads"][wildcards.samp],
    output:
        krak = join(outdir, "classification/{samp}.krak"),
        krak_report = join(outdir, "classification/{samp}.krak.report")
    params:
        db = config['database'],
        paired_string = get_paired_string(),
        confidence_threshold = confidence_threshold
    threads: 16
    resources:
        mem_mb=128000,
    singularity: "docker://quay.io/biocontainers/kraken2:2.1.2--pl5262h7d875b9_0"
    shell: """
        s5cmd cp 's3://latch-public/test-data/4034/kraken_test/db/*' {params.db} &&\

        time kraken2 --db {params.db} --threads 16 --output {output.krak} \
        --report {output.krak_report} {params.paired_string} {input.reads} \
        --confidence {params.confidence_threshold} --use-names
        """
```

### Solution

Modify the `resources` directive of the Snakefile rule.

```python
rule kraken:
    ...
    resources:
        mem_mb=128000
        cpus=8
    ...
```

## Optimize data transfer

In a Snakemake workflow, each rule is executed on a separate, isolated machine. As a result, all input files specified for a rule are downloaded to the machine every time the rule is run. Frequent downloading of the same input files across multiple rules can lead to increased workflow runtime and higher costs, especially if the data files are large.

To optimize performance and minimize costs, it is recommended to consolidate the logic that relies on shared inputs into a single rule.

#### Example

- Inefficient example with multiple rules processing the same BAM file:

```python
rule all:
    input:
        "results/final_variants.vcf"

rule mark_duplicates:
    input:
        "data/sample.bam"
    output:
        "results/dedupped_sample.bam"
    shell:
        """
        gatk MarkDuplicates \
            -I {input} \
            -O {output} \
            -M results/metrics.txt
        """

rule call_variants:
    input:
        bam = "results/dedupped_sample.bam",
        ref = "data/reference.fasta"
    output:
        "results/raw_variants.vcf"
    shell:
        """
        gatk HaplotypeCaller \
            -R {input.ref} \
            -I {input.bam} \
            -O {output}
        """

rule filter_variants:
    input:
        "results/raw_variants.vcf"
    output:
        "results/final_variants.vcf"
    shell:
        """
        gatk VariantFiltration \
            -V {input} \
            -O {output} \
            --filter-name "QD_filter" \
            --filter-expression "QD < 2.0"
        """
```

### Solution

Instead of having separate rules processing the BAM file for marking duplicates, calling variants, and filtering variants, we consolidate the logic into a single rule, reducing redundant data downloads.

```python
# Efficient Example - Consolidated logic to minimize input data downloads
rule process_and_call_variants:
    input:
        bam = "data/sample.bam",
        ref = "data/reference.fasta"
    output:
        vcf = "results/final_variants.vcf",
        dedupped_bam = temp("results/dedupped_sample.bam"),
        raw_vcf = temp("results/raw_variants.vcf")
    shell:
        """
        # Mark duplicates using GATK
        gatk MarkDuplicates \
            -I {input.bam} \
            -O {output.dedupped_bam} \
            -M results/metrics.txt

        # Call variants using GATK HaplotypeCaller
        gatk HaplotypeCaller \
            -R {input.ref} \
            -I {output.dedupped_bam} \
            -O {output.raw_vcf}

        # Filter variants using GATK VariantFiltration
        gatk VariantFiltration \
            -V {output.raw_vcf} \
            -O {output.vcf} \
            --filter-name "QD_filter" \
            --filter-expression "QD < 2.0"
        """
```
