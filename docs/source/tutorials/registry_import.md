# Tutorial: Import Sample Sheet from Registry

[Latch Registry](https://latch.wiki/what-is-registry) is a flexible sample management system that links files on Latch Data with metadata.

![The spreadsheet interface from Latch Registry that shows files and metadata](../assets/registry/registry.png)

Bioinformatics workflows, such as bulk or single-cell RNA-seq, use sample sheets as input to specify the data for analysis. The SDK provides a simple way to import sample sheets from Latch Registry to the workflow. 

In this tutorial, we will examine a workflow which assembles COVID sequencing data using Bowtie2. The workflow will take in a list of samples and sequencing reads from Registry.

## Prerequisites

* Install the [Latch SDK](../getting_started/quick_start.md)
* To follow along, clone [the GitHub repository here](https://github.com/latchbio/assembly-registry-wf).

## Define a Sample Sheet in the SDK
A sample sheet component is defined as a list of `dataclass`es in the SDK. 

First, let's define a task called `assembly_task` that accepts a single `dataclass` as an input parameter.

```python
from dataclasses import dataclass

@dataclass
class Sample:
    name: str
    r1: LatchFile
    r2: LatchFile

@small_task
def assembly_task(sample: Sample) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "--very-sensitive-local",
        "-x",
        "wuhan",
        "-1",
        sample.r1.local_path,
        "-2",
        sample.r2.local_path,
        "-S",
        str(sam_file),
    ]

    ...

    output_location = f"latch:///Assembly Outputs/{sample.name}/covid_assembly.sam"

    return LatchFile(str(sam_file), output_location)
```

Next, we can define a workflow that takes in a list of `dataclass`es. The workflow will use the [`map_task` construct](../basics/map_task.md) in the SDK to parallelize the `assembly_task` across a list of inputs.

```python
@workflow(metadata)
def assemble_and_sort(samples: List[Sample]) -> List[LatchFile]:
    return map_task(assembly_task)(sample=samples)
```

Now that we have set up the workflow logic, we can customize the workflow interface to display a sample sheet. To do so, we can set the `samplesheet` flag of `LatchParameter` equal to `True`.

```python
"""The metadata included here will be injected into your interface."""
metadata = LatchMetadata(
    display_name="Assemble FastQ Files (Registry Sample Sheet Version)",
    documentation="your-docs.dev",
    author=LatchAuthor(
        name="Author",
        email="author@gmail.com",
        github="github.com/author",
    ),
    repository="https://github.com/your-repo",
    license="MIT",
    parameters={
        "samples": LatchParameter(
            display_name="Sample sheet",
            samplesheet=True, # <======= flag to display sample sheet component
            description="A list of samples and their sequencing reads",
        )
    },
    tags=[],
)
```

To preview what the workflow interface looks like, you can type:
```bash
latch preview <path_to_workflow_directory>
```

The command will open up a new page in the browser that displays a preview of the sample sheet component.

![A preview of the sample sheet component on the workflow GUI](../assets/registry/samplesheet.png)

When you click "Import from Registry", a new import modal will pop up. 

![A preview of the sample sheet component on the workflow GUI](../assets/registry/import.png)

Here, you can select the table of interest and samples to be used in the workflow.

![A preview of the sample sheet component on the workflow GUI](../assets/registry/sample-selection.png)

**Important Note**:

* The types of columns in a Registry table must match one-to-one with the Python types of the data class defined in SDK. 
* For example, the Registry table in the picture above has three columns: **Name**, **r1**, and **r2**, which have the types **Text**, **File**, and **File** in Registry, respectively. Similarly, the property `name`, `r1`, and `r2` in the data class `Sample` has the Python types `str`, `LatchFile`, and `LatchFile`.

![A preview of the sample sheet component on the workflow GUI](../assets/registry/match-columns.png)

To learn more about how to create columns with specific types in Registry, visit our [Registry Wiki here](https://latch.wiki/create-a-table).
