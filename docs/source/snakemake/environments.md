# Environments

When registering a Snakemake workflow on Latch, we need to build a single container image containing all your runtime dependencies and the Latch packages. By default, all tasks (including the JIT step) will run inside this container.

To generate a Dockerfile with all the Latch-specific dependencies, run the following command from inside your workflow directory:

```console
latch dockerfile . --snakemake
```

Inspect the resulting Dockerfile and add any runtime dependencies required for your workflow.

## Configuring Task Environments

Sometimes, it is preferable to use isolated environments for each Snakemake rule using the `container` and `conda` [Snakemake directives](https://snakemake.readthedocs.io/en/stable/snakefiles/deployment.html#running-jobs-in-containers) instead of building one large image.

Typically, when using these directives, we must pass the `--use-conda` and `--use-singularity` flags to the `snakemake` command to configure which environment to activate. Similarly, to configure your environment on Latch, add the `env_config` field to your workflow's `SnakemakeMetadata` object. For example,

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
    ...
)
```

**Note**: If no `env_config` is defined, Snakemake tasks on Latch will NOT use containers or conda environments by default.

## Using Private Container Registries

When executing Snakemake workflows in containers, the container images may exist in a private registry that the Latch cloud cannot access. Downloading images from private registries at runtime requires two steps:

1. Upload your private container registry's password/access token to the Latch platform. See [Storing and using Secrets](../basics/adding_secrets.md).
2. Add the `docker_metadata` field to your workflow's `SnakemakeMetadata` object so the workflow engine knows where to pull your credentials. For example:

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
    env_config=EnvironmentConfig(
      use_conda=False,
      use_container=True,
    ),
    docker_metadata=DockerMetadata(
      username="user0",
      secret_name="LATCH_SECRET_NAME",
    ),
    ...
)
```

**Note**: the `secret_name` field specifies the name of the Latch Secret uploaded in step #1, NOT the actual registry password.
