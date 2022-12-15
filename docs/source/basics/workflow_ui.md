# Workflow GUI

[Latch Console](https://console.latch.bio) automatically generates a web interface for each workflow based on the workflow function signature and optional metadata:

![Metadata UI](/assets/workflow_ui/all_meta.png)

- The parameter [input widget](#widget-reference) is determined by the type anotation

- The workflow and parameter descriptions are read from the workflow function docstring. [Multiple popular formats are supported](https://github.com/rr-/docstring_parser) but [the Google docstring format](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) is preferred.

  Workflow description example:
  ![Workflow description UI](/assets/workflow_ui/description.png)

  Parameter description example:
  ![Parameter description UI](/assets/workflow_ui/param_description.png)

- Additional settings can be set using {class}`~latch.types.metadata.LatchMetadata`, {class}`~latch.types.metadata.LatchParameter`, and [annotated types](#annotated-types)

- For ease of development, the generated interface can be [previewed using the command line](/subcommands#latch-preview) without registering a new version of the workflow

## Annotated Types

Some parameter widgets e.g. lists can contain multiple values and so generate child widgets. Metadata can be attached to child widgets by {data}`annotating <typing.Annotated>` the inner types with {class}`~latch.types.metadata.LatchParameter`.

Metadata can be attached to any type within a parameter type annotation, including within lists, dataclasses, and unions.

## Custom Parameter Flow

## Widget Reference

## Example of All Major Features

Source code:

```python
from typing import Annotated

...

meta = LatchMetadata(
    display_name="Test",
    author=LatchAuthor(name="John Doe"),
    parameters={"xs": LatchParameter(display_name="List Input")},
)

@workflow(meta)
def test(
    xs: List[
        Annotated[str, LatchParameter(appearance_type=LatchAppearanceType.paragraph)]
    ]
):
    ...
```

Result:

### Customizing the Sidebar

To use `LatchMetadata`, create a singleton instance of a `LatchMetadata` object as follows:

```python
from latch.types import LatchMetadata, LatchAuthor

metadata = LatchMetadata(
    display_name="My Workflow",
    documentation="https://github.com/author/my_workflow/README.md",
    author=LatchAuthor(
        name="Workflow Author",
        email="licensing@company.com",
        github="https://github.com/author",
    ),
    repository="https://github.com/author/my_workflow",
    license="MIT",
)
```

---

## Adding Documentation to your Workflow

While most of the metadata of a workflow will be encapsulated in a LatchMetadata object, we still require a docstring in the body of the workflow function which specifies both a short and long-form description.

### One Line Description

The first line of the workflow function docstring will get rendered in the sidebar of the workflow and the workflow explore tab as a brief description of your workflow's functionality. Think of this as summarizing the entirety of your workflow's significance into a single line.

We recommend limiting your workflow description to one sentence, as longer descriptions are only partially rendered on the Workflows page.

```python
@workflow
def foo(
    ...
):
    """This line is a short workflow description, displayed in the explore tab and sidebar.

    ...
    """
    ...
```

Example:

```python
@workflow
def rnaseq(
    ...
):
    """Perform alignment and quantification on Bulk RNA-Sequencing reads.

    ...
    """
    ...
```

![Short Description](../assets/ui/one-line%20description.png)

### Long Form Description

The body of the workflow function docstring is where you write long-form markdown documentation. This markdown will get rendered in the dedicated workflow "About" tab on your interface. Feel free to include links, lists, code blocks, and more.

```python
@workflow
def foo(
    ...
):
    """This line is a short workflow description, displayed in the explore tab

    This line starts the long workflow description in markdown, displayed in
    this workflow's about tab

    Lists
    - item1
    - item2
    - item3

    ### headers

    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
    tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,
    quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
    consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse
    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat
    non proident, sunt in culpa qui officia deserunt mollit anim id est
    laborum.
    """
    ...
```

Example:

```python
@workflow
def rnaseq(
    ...
):
    """Perform alignment and quantification on Bulk RNA-Sequencing reads

    Bulk RNA-Seq (Alignment and Quantification)
    ----
    This workflow will produce gene and transcript counts from bulk RNA-seq
    sample reads.

    # Workflow Anatomy

    # Disclaimer

    This workflow assumes that your sequencing reads were derived from *short-read
    cDNA seqeuncing* ...

    # Brief Summary of RNA-seq

    This workflow ingests short-read sequencing files (in FastQ format) that came
    from the following sequence of steps[^1]:
      - RNA extraction from sample
      - cDNA synthesis from extracted RNA
      - adaptor ligation / library prep
      - (likely) PCR amplification of library
      - sequencing of library
    You will likely end up with one or more FastQ files from this process that hold
    the sequencing reads in raw text form. This will be the starting point of our
    workflow.
    ...
    """
```

![Long Description](../assets/ui/long-form%20description.png)

---

### Customizing Parameter Presentation

Any input of the main `@workflow` function can be added & customized on the front end display for ingesting user values in the browser. To add a workflow parameter to the front end simply add a `LatchParameter` object to your `LatchMetadata` object's parameter dictionary:

```python
from latch.types import LatchParameter, LatchAppearanceType, LatchRule

...

# Assuming you have created a LatchMetadata object named `metadata`
metadata.parameters['param_0'] = LatchParameter(
    display_name="Parameter 0",
    description="This is parameter 0",
    hidden=False,
)

...

@workflow(metadata)
def wf(
    param_0: int, # any of the supported types would also work here
    ...
)
```

When a workflow is registered, each workflow parameter will receive a frontend component to ingest values in the browser.

Each key in `metadata.parameters` must be the name of one of the parameters of the workflow, and so the corresponding `LatchParameter` object describes that specific parameter. A `LatchParameter` can take a myriad of keyword arguments at construction time, each of which are briefly described below.

- `display_name` (str): A human-readable, descriptive name of the parameter,
- `description` (str): A short description of the role of the parameter within the workflow, to be displayed when hovered over in a tooltip,
- `hidden` (boolean): A boolean for whether or not the parameter should be hidden by default,
- `section_title` (str): If provided, the specified parameter will start a new section of the given name,
- `placeholder` (str): What placeholder to put inside the input form for the parameter if no value is present,
- `comment` (str): A comment about the parameter,
- `output` (boolean): Whether this parameter is an output directory (to disable path existence checks),
- `batch_table_column` (boolean): Whether this parameter should have a column to itself in the batch table at the top of the parameters page,
- `appearance_type`: Either `LatchAppearanceType.line` or `LatchAppearanceType.paragraph`, which style to render text inputs as.
- `rules`: A list of `LatchRule`s which consist of a regular expression and a message. If provided, an input must match all given regexes in order to appear valid in the front end - if it fails to match one of the regexes, the corresponding message is displayed.

See below for a parameter display that uses all options mentioned:

```python
from latch.types import LatchMetadata, LatchAuthor, LatchRule, LatchAppearanceType

metadata = LatchMetadata(
    parameters={
        "read1": LatchParameter(
            display_name="Read 1",
            description="Paired-end read 1 file to be assembled.",
            hidden=True,
            section_title="Sample Reads",
            placeholder="Select a file",
            comment="This is a comment",
            output=False,
            appearance_type=LatchAppearanceType.paragraph,
            rules=[
                LatchRule(
                    regex="(.fasta|.fa|.faa|.fas)$",
                    message="Only .fasta, .fa, .fas, or .faa extensions are valid"
                )
            ],
            batch_table_column=True,  # Show this parameter in batched mode.
        ),
)

@workflow(metadata)
def wf(read1: LatchFile):
    ...
```

### How Python types of paramters translate to the UI

Latch parses the Python type of your workflow parameters to generate the appropriate interface.

Below is a list of examples of Python types and how they translate to the UI:

1. LatchFile

```python
from latch.types import LatchFile
from typing import Optional

...
@workflow
def bactopia_wf(
    ...
    fastq_one: Optional[LatchFile] = None,
    ...
)
```

![LatchFile](../assets/ui/optional-latch-file.png)

`LatchFile` receives a button that allows users of the workflow to select data from their Latch account. The `Optional` type renders the toggle for `fastq_one`. When the toggle is turned on, Latch automatically detects the empty path and throws a warning. Additionally, you can set the default value to the path to `None`.

2. LatchDir

```python
from latch.types import LatchDir

...
@workflow
def bactopia_wf(
    ...
    output_dir: LatchDir,
    ...
)
```

![LatchDir](../assets/ui/latchdir.png)

3. Boolean

```python
hybrid: bool = False,
```

![Boolean](../assets/ui/boolean.png)

4. Enum

```python
from enum import Enum

# You must define your Enum as a python class before using it as an annotation.
class SpeciesGenomeSize(Enum):
    mash = "mash estimate"
    min = "min"
    median = "median"
    mean = "mean"
    max = "max"

...
@workflow
def bactopia_wf(
    ...
    species_genome_size: SpeciesGenomeSize,
    ...
)
```

![Enum](../assets/ui/enum.png)

5. Int

```python
@workflow
def bactopia_wf(
    ...
    coverage: int,
    ...
)
```

![int](../assets/ui/int.png)

6. Str

```python
@workflow
def bactopia_wf(
    ...
    sample_name: str = "sample1"
    ...
)
```

![str](../assets/ui/str.png)

7. List

```python
from latch.types import LatchFile
from typing import List

@workflow
def rnaseq(
    sample_identifiers: List[str],
    sample_reads: List[LatchFile]
):
...
```

![List](../assets/ui/list.png)

When `List` is used, Latch generates a plus sign, where users can add additional values of the same type. For `LatchFile`s specifically, an additional button **Bulk Add Files** is generated, allowing users to select multiple files at once.

8. Dataclass

If you want to handle file references and their associated metadata as an input to your workflow, you may want to use a `dataclass`.

```python
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class Sample:
    name: str
    fastq: LatchFile

@workflow
def rnaseq(
    samples: List[Sample]
):

```

![List of class](../assets/ui/dataclass.png)

Here, we are passing a list of `Sample`s as the input. On the Latch interface, when a user clicks the `+ Sample` button, a new block will be added with two parameters of the Python class `name` and `fastq`.

---

## Adding your workflow to a biological domain on Latch

For public workflows, you may want to classify your workflow to a biological domains to make it easier for future users to discover.

To do so, you can use the `tags` property of `LatchMetadata`.

```python
metadata = LatchMetadata(
    ...
    tags=["NGS", "MAG"],
    ...
)
```

Below is a list of commonly used domains on Latch. For best practices, you should tag your workflow with an existing domain instead of creating a new one.

- Aggregator
- COVID
- CRISPR
- Epigenetics
- Guide Design
- Library Screen
- MAG
- NGS
- Nextflow

![Tags](../assets/ui/tags.png)

## See Also

- [`latch preview`](/subcommands#latch-preview)
