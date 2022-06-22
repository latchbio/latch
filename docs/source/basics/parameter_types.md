# Parameter Types

The Latch SDK supports first-class static typing. All input parameters and
output values must be annotated with types in the function signature.

These type annotations validate task values and guide construction of
parameter-specific frontend components when your interface is generated at
registration.

---

## Supported Types

Below is an exhaustive list of supported types currently shipped with the SDK.

### Integers

```python
@task
def foo(
  a: int = 10
):
  ...
```

### Floats

```python
@task
def foo(
  a: float = 10.0
):
  ...
```

### Strings

```python
@task
def foo(
  a: str = "bio"
):
  ...
```

### Files

```python
@task
def foo(
  a: LatchFile = LatchFile("/root/data.txt")
):
  ...
```

### Directories

```python
@task
def foo(
  a: LatchDir = LatchDir("/root/test_data/")
):
  ...
```

### Enums

```python
from enum import Enum

# You must define your Enum as a python class before using it as an annotation.
class Statistic(Enum):
    min = "min"
    median = "median"
    mean = "mean"
    max = "max"

@task
def foo(
  a: Statistic = Statistic.min
):
  ...
```

### Unions

Along with the previous basic types, `Union` and `Optional` types from Python's
`typing` module give extra flexibility.

Recall that `Optional[T] = Union[None, T]`.

```python
from typing import Union, Optional


@task
def foo(
  a: Union[int, LatchFile] = LatchFile("/root/data.txt"),
  b: Optional[float] = None
):
  ...
```

### Lists

Finally, we currently support a single collection type which is `List[T]`, also
provided by the `typing` module.

```python
from typing import List, Optional

@task
def foo(
  a: List[str] = ["AUG", "GGG"],
  b: List[Optional[int]] = [1, None, 3, 4, None, 6]
):
  ...
```

## Setting Default Values

Notice that every parameter in the above list of types had a default value. This
default value will be displayed to the user in the frontend interface and will
be passed by default if no change is made. You do not have to provide a default
value, but it helps document parameters and provide semantic context to users.

## Example

Here is the function header for Bactopia, a workflow available on Latch. To see
how these inputs translate into a user interface, check out
[Bactopia](https://console.latch.bio/se/bactopia) on Latch.

```python
@large_task
def bactopia_tsk(
    fastq_one: Optional[LatchFile],
    fastq_two: Optional[LatchFile],
    input_dir: Optional[LatchDir],
    output_dir: LatchDir,
    sample_name: List[Union[str, int]],
    genome_size: Optional[int],
    species: Optional[Species],
    species_genome_size: Optional[SpeciesGenomeSize],
    ask_merlin: bool = False,
    coverage: int = 100,
    hybrid: bool = False,
    skip_logs: bool = False,
    skip_fastq_check: bool = False,
    skip_qc: bool = False,
    skip_error_correction: bool = False,
    no_miniasm: bool = False,
    skip_pseudogene_correction: bool = False,
    skip_adj_correction: bool = False,
    skip_prodigal_tf: bool = False,
    rawproduct: bool = False,
    centre: str = "Bactopia",
    min_contig_length: int = 500,
    amr_plus: bool = False,
) -> LatchDir:
    # example opening a LatchFile
    with open(Path(fastq_one), "w") as f:
        lines = f.readlines() 
    
        # ... Logic Here ...
    
        local_output_dir = Path("/root/outputs")
  
        # example returning Flyte directory
        return LatchDir(
            str(local_output_dir.resolve()),
            remote_directory=_fmt_dir(output_dir.remote_source),
        )


@workflow
def bactopia_wf(
    output_dir: LatchDir,
    sample_name: List[Union[str, int]] = "sample1",
    fastq_one: Optional[LatchFile] = None,
    fastq_two: Optional[LatchFile] = None,
    input_dir: Optional[LatchDir] = None,
    genome_size: Optional[int] = None,
    species: Species = Species.none,
    species_genome_size: SpeciesGenomeSize = SpeciesGenomeSize.mash,
    ask_merlin: bool = False,
    coverage: int = 100,
    hybrid: bool = False,
    skip_logs: bool = False,
    skip_fastq_check: bool = False,
    skip_qc: bool = False,
    skip_error_correction: bool = False,
    no_miniasm: bool = False,
    skip_pseudogene_correction: bool = False,
    skip_adj_correction: bool = False,
    skip_prodigal_tf: bool = False,
    rawproduct: bool = False,
    amr_plus: bool = False,
    centre: str = "Bactopia",
    min_contig_length: int = 500,
) -> LatchDir:

    return bactopia_tsk(
        fastq_one=fastq_one,
        fastq_two=fastq_two,
        input_dir=input_dir,
        output_dir=output_dir,
        sample_name=sample_name,
        genome_size=genome_size,
        species=species,
        species_genome_size=species_genome_size,
        ask_merlin=ask_merlin,
        coverage=coverage,
        hybrid=hybrid,
        skip_logs=skip_logs,
        skip_fastq_check=skip_fastq_check,
        skip_qc=skip_qc,
        skip_error_correction=skip_error_correction,
        no_miniasm=no_miniasm,
        skip_pseudogene_correction=skip_pseudogene_correction,
        skip_adj_correction=skip_adj_correction,
        skip_prodigal_tf=skip_prodigal_tf,
        rawproduct=rawproduct,
        centre=centre,
        min_contig_length=min_contig_length,
        amr_plus=amr_plus,
    )

```
