# Metadata

The Snakemake framework was designed to allow developers to both define and execute their workflows. This often means that the workflow parameters are sometimes ill-defined and scattered throughout the project as configuration values, static values in the `Snakefile`, or command line flags.

To construct a graphical interface from a Snakemake workflow, the file parameters need to be explicitly identified and defined so that they can be presented to scientists through a web application.

## Generating Latch Metadata

The `latch_metadata` folder holds these parameter definitions.

To generate Latch metadata from a config file, type:

```console
latch generate-metadata <path_to_config.yaml>
```

The command automatically parses the existing `config.yaml` file in the Snakemake repository to create a `SnakemakeMetadata` object. Below is an explanation of the most relevant fields:

#### output_dir

A `LatchDir` object that points to the location in Latch Data where the Snakemake outputs will be stored after the workflow has finished executing.

#### parameters

Input parameters to the workflow. The Latch Console will expose these parameters to scientists before they execute the workflow.

#### file_metadata

Every input parameter of type `LatchFile` or `LatchDir` must have a corresponding `SnakemakeFileMetadata` in the `file_metadata` field. The `SnakemakeFileMetadata` object provides important metadata about the file to the workflow, such as:

1. `path`: The local path inside the container where the workflow engine will copy Latch Data files/directories before the job executes
2. `config`: If `True`, exposes the local file path in the Snakemake config
3. `download`: If `True`, downloads the file in the JIT step instead of creating an empty file.
   **Note**: To limit network consumption, only files, such as configuration files, used by the Snakefile at compilation time should set this field to `True`.

## Example

Below is an example `config.yaml` file and corresponding latch metadata after running `latch generate-metadata`

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

After registering the above workflow to Latch, you will see an interface like the one below:

![Snakemake workflow GUI](../assets/snakemake/metadata.png)
