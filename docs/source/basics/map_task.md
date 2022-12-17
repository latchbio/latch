# Map Task

There are many pipelines in bioinformatics that require running a processing
step in parallel and aggregating their outputs at the end for downstream
analysis. A prominent example of this is bulk RNA-sequencing, where
alignment is performed to produce transcript abundances per sample, and gene
counts of all samples are subsequently merged. Having a single count matrix
makes it convenient to use in downstream steps, such as differential gene
expression analysis. Another example is performing FastQC on multiple samples
and summarizing the results in a MultiQC report.

The Latch SDK introduces a construct called `map_task` to help parallelize a
task across a list of inputs. This means you can run multiple instances of
the task at the same time inside a single workflow, providing valuable
performance gains.

Let's look at a simple example below!

First, import `map_task` into your workflow:

```python
from typing import List

from latch import map_task, small_task, workflow
```

Next, define a task to use in the map task.
> Note: A map task can only accept **one input** and produce **one output**.

```python
@small_task
def a_mappable_task(a: int) -> str:
    inc = a + 2
    stringified = str(inc)
    return stringified
```

Let's also define a task that collects the mapped output and returns a string:

```python
@small_task
def coalesce(b: List[str]) -> str:
    coalesced = "".join(b)
    return coalesced
```

We can run `a_mappable_task` across a collection of inputs using the `map_task` function. This function takes in `a_mappable_task` and returns a mapped version of that task. This mapped version takes as input a list of inputs to `a_mappable_task` , and returns a list of the outputs of `a_mappable_task` run on all inputs in the list in parallel.

```python
@workflow
def my_map_workflow(a: typing.List[int]) -> str:
    mapped_out = map_task(a_mappable_task)(a=a)
    coalesced = coalesce(b=mapped_out)
    return coalesced
```

That's it! You've successfully defined `a_mappable_task` that is passed to a
`map_task()` and run repeatedly on a list of inputs in parallel. You have also
defined a `coalesce` task to collect the list of outputs from the mapped task
and returns a string.

## Map a Task with Multiple Inputs

You may want to map a task with multiple inputs.

For example, the task below takes in 2 inputs, a base and a DNA sequence, and
returns the percentage of that base in the sequence:

```python
@small_task
def count_task(base: str, dna_sequence: str) -> float: 
    return dna_sequence.count(base) / len(dna_sequence) * 100
```

But we only want to map this task with the `base` input while the
`dna_sequence` stays the same. Since a map task accepts only one input, we can
do this by creating a new task that prepares the map task’s inputs.

We start by putting the inputs in a Dataclass and `dataclass_json`.

```python
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class MapInput:
    base: str
    dna_sequence: str
```

Let's also define our helper task to prepare the map task’s inputs.

```python
@small_task
def prepare_map_inputs(list_base: List[str], dna_sequence: str) -> List[MapInput]:
    return [MapInput(base, dna_sequence) for base in list_base]
```

We now refactor the original `count_task`. Instead of 2 inputs, `count_task`
has a single input:

```python
@small_task
def mappable_task(input: MapInput) -> float:
    return input.dna_sequence.count(input.base) / len(input.dna_sequence) * 100
```

Let's use the new `mappable_task` in our workflow:

```python
@workflow
def count_wf(list_base: List[str] = ["A", "T", "C", "G"], dna_sequence: str = "AAAATTTCCGG") -> List[float]:
    prepared = prepare_map_inputs(list_base=list_base, dna_sequence=dna_sequence)
    return map_task(mappable_task)(input=prepared)
```

Great! Now, we are able to use the `count_wf` to spin up four tasks in
parallel. The `map_task` returns a list of four floats, each of which is the
percentage of base pair in the DNA sequence.

---

## Bonus: Learning through a Biological Example

In the example below, we walk through a practical example of how we can use the
map task construct to run FastQC on multiple samples and summarize their
results in a MultiQC report.

First, we define a Dataclass that contains a sample name and its associated
FastQ file:

```python
@dataclass_json
@dataclass
class Sample:
    sample_name: str
    fastq: LatchFile
```

Then, we create a task to run FastQC on a single sample and output the result
under the **FastQC Results** folder on Latch.

```python
@small_task
def fastqc_task(sample) -> LatchDir:

    outdir = Path("/root/fastqc_result").resolve()
    outdir.mkdir(exist_ok=True)

    _fastqc_cmd = [
        "/root/FastQC/fastqc", 
        sample.fastq.local_path, 
        f"--outdir={outdir}"
    ]

    subprocess.run(_fastqc_cmd, check=True)

    return LatchDir("/root/fastqc_result", f"latch:///FastQC Results/{sample.sample_name}")
```

> **Concept check**: Note how this task will later be mapped across a list of
samples. Therefore, the task is defined to accept one input and return one
output.

Next, define a second task to run MultiQC on a given directory for analysis
logs and compiles a HTML report.

```python
@small_task
def multiqc_task(fastqc_results: List[LatchDir]) -> LatchDir:

    outdir = Path("/root/multiqc_results").resolve()
    outdir.mkdir(exist_ok=True)

    fastqc_dirs = [result.local_path for result in fastqc_results]

    _multiqc_cmd = ["multiqc"] + fastqc_dirs + ["-o", outdir]

    subprocess.run(_multiqc_cmd, check=True)

    return LatchDir(outdir, "latch:///MultiQC Results")
```

> **Concept check**: Because the map task will return a list of `LatchDir`s,
each of which contains an individual sample's FastQC results, the
`multiqc_task` needs to also accept a list of `LatchDir`s.

Finally, we can specify our workflow, which accepts a list of `Sample`s and
returns a single directory with the MultiQC report:

```python
@workflow(metadata)
def fastqc_multiqc_wf(samples: List[Sample]) -> LatchDir:
    fastqc_results = map_task(fastqc_task)(sample=samples)  # returns List[LatchDir]
    return multiqc_task(fastqc_results=fastqc_results) # accepts a List[LatchDir] and return a single LatchDir with the MultiQC result
```
