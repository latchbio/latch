# Latch SDK Registry Integration

Classes and utility methods for interacting with [Latch Registry](https://latch.wiki/what-is-registry) are provided out of the box in the Latch SDK.
These are used during workflow execution to read from and write values to Tables in Registry.

Among others, classes for `Accounts`, `Projects`, `Tables`, and `Records` are provided.

## Structure

Just like its web counterpart, this API is designed hierarchically, so that `Account`s can list their constituent `Project`s, `Project`s can list their constituent `Table`s, and `Table`s can list their constituent `Records`.

Each of these can also be instantiated directly by their ID, providing that the workspace in which the execution is running has access to them.

For more detailed documentation about all of these classes' behavior/methods, see their respective pages.

## Usage Example

The following is a typical example of usage.

```python
import os
from pathlib import Path

from latch.registry.table import Table
from latch.resources.tasks import small_task
from latch.types.file import LatchFile


@small_task
def registry_task(record_name: str, file: LatchFile) -> LatchFile:
    file_path = Path(file.local_path)
    file_size = os.stat(file_path).st_size

    tbl = Table(id="1234")
    with tbl.update() as updater:
        updater.upsert_record(
            record_name,
            {
                "File": file,
                "Size": file_size,
            },
        )

    return file


```

Inside a task, a `Table` object is instantiated with id `"1234"` and a record with name `record_name` in this table is updated (or inserted if necessary) with the provided column values.
