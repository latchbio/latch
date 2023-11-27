# Workflow Automations

Note: This document is a work in progress and is subject to change.

**Description**

Automations allow you to automatically run a workflow on top of folders in Latch Data based on specific triggers such as when files are added to folders. Automations consist of a trigger, automation workflow and your target workflow.

* _Trigger_ allows you to specify a target directory to watch, the event which kicks off a workflow, and a timer for how long to wait to start the workflow after the last file has been added to the target directory. Currently, automations only support file addition events.
* _Automation workflow_ runs when the automation triggers. We have supplied a template workflow which reads children of the target folder, runs your _target workflow_ on children and updates which children have been processed in a table your specify inside [Latch Registry](../registry/overview.md).
* _Target workflow_ contains the logic of how to process the files in the child directories.

Below, we will walk through the process of creating an automation on Latch. We assume that you understand how to write and register [Workflows](../basics/what_is_a_workflow.md) on Latch.

**Prerequisites:**
* An existing Table in [Latch Registry](https://latch.wiki/what-is-registry)
* An target folder in [Latch Data](https://console.latch.bio/data)

---

## 1: Create Automation Workflow

Clone the [Automation Workflow Template](https://github.com/latchbio/automation-wf) and navigate to the `automation-wf/wf` directory.

```shell-session
$ git clone git@github.com:latchbio/automation-wf.git
Cloning into 'automation-wf'...
remote: Enumerating objects: 33, done.
remote: Counting objects: 100% (33/33), done.
remote: Compressing objects: 100% (24/24), done.
remote: Total 33 (delta 9), reused 28 (delta 6), pack-reused 0
Receiving objects: 100% (33/33), 8.52 KiB | 1.42 MiB/s, done.
Resolving deltas: 100% (9/9), done.

$ cd automation-wf/wf
```

File Tree:
```shell-session
├── Dockerfile
├── README.md
├── version
└── wf
    ├── __init__.py
    └── automation.py
```

## 2. Configure Automation Workflow Parameters

To specify the child workflow and the registry table with processed children, configure the following parameters in `wf/__init__.py`:

* `wf_id`: The ID of the workflow you want to run.
* `table_id`: The ID of the table that stores metadata for this automation.

Get `wf_id` for the target workflow by going to `Workflows` page on Latch Console, clicking on your workflow and getting the ID from the sidebar.

You will need to create a table to record processed children directories. Go to `Registry` on Latch Console, and create a new table in one of your existing projects. Get the ID of the table from the sidebar and pass it as `table_id`.

### Get Workflow ID
![Workflow ID](../assets/automation//get-workflow-id.png)

### Get Table ID
![Table ID](../assets/automation//get-table-id.png)

```python
# __init__.py
from latch.resources.workflow import workflow
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter

from wf.automation import automation_task

...

@workflow(metadata)
def automation_workflow(input_directory: LatchDir, automation_id: str) -> None:
    automation_task(
        input_directory=input_directory,
        wf_id="FIXME",  # fixme: change wf_id to desired workflow
        table_id="FIXME",  # fixme: change table_id to desired registry table
    )
```

## 3. Configure Target Workflow Parameters

You can configure the parameters for your workflow in `wf/automation.py`:

* output_directory: The directory where the output of the workflow will be stored.

```python
# automation.py
import os
import uuid
from typing import Set
from urllib.parse import urljoin

import requests
from latch.account import Account
from latch.registry.table import Table
from latch.resources.tasks import small_task
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile

...

@small_task
def automation_task(input_directory: LatchDir, wf_id: str, table_id: str) -> None:
    automation_table = Table(table_id)

    if automation_table.get_columns().get("Resolved directories", None) is None:
        with automation_table.update() as automation_table_updater:
            automation_table_updater.upsert_column("Resolved directories", LatchDir)

    resolved_directories: Set[str] = set()
    for page in automation_table.list_records():
        for _, record in page.items():
            value = record.get_values()["Resolved directories"]
            assert isinstance(value, LatchDir)
            resolved_directories.add(str(value))

    output_directory = LatchOutputDir(
        path="latch://<FIXME>"  # fixme: change to remote path of desired output directory
    )
...
```

Right now, the automation workflow is configured to pass `input_directory` and `output_directory` as parameters to the child workflow.

You can modify the `launch_workflow` function to pass any parameters for your workflow by modifying the `data` dictionary. You can find the exact object to pass as `data` by going to an existing execution of your workflow, clicking on `inputs` and copying your workflow parameters inside the `literal` object.

### Get the Parameters for Your Workflow

![Table ID](../assets/automation/get-workflow-parameters.png)


Code to change:

```python
# automation.py

def launch_workflow(
    wf_id: str,
    input_directory: LatchDir,
    output_directory: LatchOutputDir,
) -> None:
    ...
    data = {
        ...
        "params": {
            "input_directory": {
                "scalar": {
                    "blob": {
                        "metadata": {"type": {"dimensionality": "MULTIPART"}},
                        "uri": input_directory.remote_path,
                    }
                }
            },
            "output_directory": {
                "scalar": {
                    "blob": {
                        "metadata": {"type": {"dimensionality": "MULTIPART"}},
                        "uri": output_directory.remote_path,
                    }
                }
            },
        },
    }
    ...
```


## 4. Register Workflow

Register the workflow to your Latch workspace.

```
$ latch register --remote --yes automation-wf
```

### Get Workflow ID

Once the workflow has been registered. Go to [Latch Console](https://console.latch.bio/workflows) and click into workflow you just registered. The workflow ID will be displayed in the sidebar.

![Workflow ID](../assets/automation//get-workflow-id.png)

## 5. Create Automation

Navigate to [Automations](https://console.latch.bio/automations) tab via **Worfklows** > **Automations** and click on the **Create Automation** button.

Input an **Automation Name** and **Description**.

Next, select a folder where files/folders will be uploaded using the `Select Target` button. Any items uploaded to this folder will trigger the specified workflow.

Finally, input the Workflow ID you obtained in the previous step.

