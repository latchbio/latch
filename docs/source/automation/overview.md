# Creating an Automaiton (WIP)

Note: This document is a work in progress and is subject to change.

In this document, we will walk through the process of creating an automation on Latch. We assume that you understand how to write and register [Worfklows](../basics/what_is_a_workflow.md) on Latch.

**Prerequisite:**
* [Automation Workflow Template](https://github.com/latchbio/automation-wf)
* An existing Table in [Latch Registry](https://latch.wiki/what-is-registry)
* An target folder in [Latch Data](https://console.latch.bio/data)

---

## 1: Create Automation Workflow
Clone the [Automation Workflow Template](https://github.com/latchbio/automation-wf) and naviagate to the `automation-wf/wf` directory.

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

## 2. Change Parameters

Configure the following parameters in `wf/__init__.py`:

* `wf_id`: The ID of the workflow you want to run.
* `table_id`: The ID of the table that stores metadata for this automation

```python
# __init__.py

...
@workflow(metadata)
def automation_workflow(input_directory: LatchDir, automation_id: str) -> None:
    automation_task(
        input_directory=input_directory,
        wf_id="FIXME",  # fixme: change wf_id to desired workflow
        table_id="FIXME",  # fixme: change table_id to desired registry table
    )
```

Configure the followin parameters in `wf/automation.py`:

* output_directory: The directory where the output of the workflow will be stored.

```python
# automation.py

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

Optionally, you can modify the `launch_workflow` function to match the parameters of your workflow.

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


## 3. Register Workflow

Register the workflow to your Latch workspace.

```
$ latch register --remote --yes automation-wf
```

### Get Workflow ID

Once the workflow has been registered. Go to [Latch Console](https://console.latch.bio/workflows) and click into workflow you just registered. The workflow ID will be displayed in the sidebar.

![Workflow ID](../assets/automation//get-workflow-id.png)

## 4. Create Automation

Navigate to [Automations](https://console.latch.bio/automations) tab via **Worfklows** > **Automations** and click on the **Create Automation** button.

Input an **Automation Name** and **Description**.

Next, select a folder where files/folders will be uploaded using the `Select Target` button. Any items uploaded to this folder will trigger the specified workflow.

Finally, input the Workflow ID you obtained in the previous step.

