# Private Container Registries

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
