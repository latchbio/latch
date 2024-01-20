# Environments

## Configuring Conda and Container Environments

Latch's Snakemake integration supports the use of both the `conda` and `container` directives in your Snakefile. To configure which environment to run tasks in (which is typically done through the use of `--use-conda` and `--use-singularity`), add the `env_config` field to your workflow's `SnakemakeMetadata` object. For example,

```
# latch_metadata.py
from latch.types.metadata import SnakemakeMetadata, SnakemakeFileParameter, EnvironmentConfig
from latch.types.directory import LatchDir
from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter
from pathlib import Path

SnakemakeMetadata(
    display_name="snakemake_tutorial_workflow",
    author=LatchAuthor(
            name="latchbio",
    ),
    env_config=EnvironmentConfig(
      use_conda=True,
      use_container=True,
    ),
    parameters={
        "samples" : SnakemakeFileParameter(
                display_name="Sample Input Directory",
                description="A directory full of FastQ files",
                type=LatchDir,
                path=Path("data/samples"),
        ),
        "ref_genome" : SnakemakeFileParameter(
                display_name="Indexed Reference Genome",
                description="A directory with a reference Fasta file and the 6 index files produced from `bwa index`",
                type=LatchDir,
                path=Path("genome"),
        ),
    },
)
```

If there is no `env_config` defined, Snakemake tasks on Latch will use both containers and conda environments by default.

## Using Private Container Registries

When executing Snakemake workflows in containers, it is possible that the container images will exist in a private registry that the Latch cloud does not have access to. Downloading images from private registries at runtime requires two steps:

1. Upload the password / access token of your private container registry to the Latch platform. See [Storing and using Secrets](../basics/adding_secrets.md).
2. Add the `docker_metadata` field to your workflow's `SnakemakeMetadata` object so that the workflow engine knows where to pull your credentials from. For example:

```
# latch_metadata.py
from latch.types.metadata import SnakemakeMetadata, SnakemakeFileParameter, DockerMetadata
from latch.types.directory import LatchDir
from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter
from pathlib import Path

SnakemakeMetadata(
    display_name="snakemake_tutorial_workflow",
    author=LatchAuthor(
            name="latchbio",
    ),
    docker_metadata=DockerMetadata(
      username="user0",
      secret_name="LATCH_SECRET_NAME",
    ),
    parameters={
        "samples" : SnakemakeFileParameter(
                display_name="Sample Input Directory",
                description="A directory full of FastQ files",
                type=LatchDir,
                path=Path("data/samples"),
        ),
        "ref_genome" : SnakemakeFileParameter(
                display_name="Indexed Reference Genome",
                description="A directory with a reference Fasta file and the 6 index files produced from `bwa index`",
                type=LatchDir,
                path=Path("genome"),
        ),
    },
)
```

**Note**: the `secret_name` field specifies the name of the Latch Secret uploaded in step #1, NOT the actual registry password.
