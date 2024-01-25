# Metadata

The Snakemake framework was designed to allow developers to both define and execute their workflows. This often means that the workflow parameters are sometimes ill-defined and scattered throughout the project as configuration values, static values in the `Snakefile`, or command line flags.

To construct a graphical interface from a Snakemake workflow, the file parameters need to be explicitly identified and defined so that they can be presented to scientists through a web application.

The `latch_metadata` folder holds these parameter definitions.

To generate Latch metadata from a config file, type:

```console
latch generate-metadata <path_to_config.yaml>
```

The command automatically parses the existing `config.yaml` file in the Snakemake repository and creates a Python parameters file. After running the command, inspect the generated files to verify that the parameter types and file paths are what the workflow expects.

## Example

Below is an example `config.yaml` file and corresponding latch metadata.

`config.yaml`

```yaml
paths:
  sample_dir: data/samples/
  reference_dir: reference/

manifest: manifest.tsv

metadata:
  threads: 32
  num_samples: 2
```

The `latch_metadata/` folder generated from the `latch generate-metadata` command contains two files:

```
├── config.yaml
├── latch_metadata
│   └── __init__.py
│   └── parameters.py
```

```python
# latch_metadata/__init__.py
from latch.types.metadata import SnakemakeMetadata, LatchAuthor
from latch.types.directory import LatchDir

from .parameters import generated_parameters, file_metadata

SnakemakeMetadata(
    output_dir=LatchDir("latch:///your_output_directory"),
    display_name="Your Workflow Name",
    author=LatchAuthor(
        name="Your Name",
    ),
    parameters=generated_parameters,
    file_metadata=file_metadata,
)
```

```python
# latch_metadata/parameters.py
from dataclasses import dataclass
import typing

from latch.types.metadata import SnakemakeParameter, SnakemakeFileParameter, SnakemakeFileMetadata
from latch.types.file import LatchFile
from latch.types.directory import LatchDir

@dataclass
class paths:
    sample_dir: LatchDir
    reference_dir: LatchDir


@dataclass
class metadata:
    threads: int
    num_samples: int


generated_parameters = {
    'paths': SnakemakeParameter(
        display_name='Paths',
        type=paths,
    ),
    'manifest': SnakemakeParameter(
        display_name='Manifest',
        type=LatchFile,
    ),
    'metadata': SnakemakeParameter(
        display_name='Metadata',
        type=metadata,
        default=metadata(threads=32, num_samples=2),
    ),
}

file_metadata = {
    'paths': {
        'sample_dir': SnakemakeFileMetadata(
            path='data/samples/',
            config=True,
        ),
        'reference_dir': SnakemakeFileMetadata(
            path='reference/',
            config=True,
        ),
    },
    'manifest': SnakemakeFileMetadata(
        path='manifest.tsv',
        config=True,
    ),
}
```

The `parameters` field contains all input parameters the Latch Console will expose to scientists before executing the workflow.

The `file_metadata` field specifies metadata about the input files as a `SnakemakeFileMetadata` object. Every input parameter of type `LatchFile` or `LatchDir` must have a corresponding `SnakemakeFileMetadata` in the `file_metadata` field.

After registering the above workflow to Latch, you will see an interface like the one below:

![Snakemake workflow GUI](../assets/snakemake/metadata.png)
