# Workflow Automations

Note: This document is a work in progress and is subject to change.

### Description

Automations allow you to automatically run workflows on top of folders in Latch Data when triggered by specific events such as when files are added to folders. Automations consist of a *trigger*, *automation workflow*.

<!-- * _Status_ specifies if automation is active or not. Use automation sidebar to toggle status of your automation. -->

### Trigger

Automation trigger specifies the conditions after which the automation will run. It allows you to specify a target directory to watch, the _event_ which kicks off a workflow, and a _timer_.

#### Trigger Event Types

Automation events consist of a type and an event itself. Type is a high level definition of event(i.e. data has been updated), and specific event specifies what triggers automation(i.e. child has been added).

> Currently, only child addition events are supported in automations.

Available events types:

- _Data Update_ event type specifies when to run the automation if a data tree in Latch Data has been modified. Supported events for this type are:
    -  _Child Added_ event triggers if a new child has been added to the target directory at any depth. Automation will not run if the child has been modified.

#### Trigger Timer

Automation trigger timer specifies the wait period after the last file has been added to the target directory after which the workflow will run.

### Automation Workflow

This is the [workflow](../basics/what_is_a_workflow.md) that will run whenever the automation has been [triggered](#trigger).

Note: Right now, automations are limited to accepting workflows with a fixed set of parameters. Make sure to define your workflow with `input_directory` and `automation_id` as parameters.

```python
# __init__.py

from latch.resources.workflow import workflow
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter

from wf.automation import automation_task

metadata = LatchMetadata(
    display_name="Automation Workflow",
    author=LatchAuthor(
        name="Your Name",
    ),
    parameters={
        "input_directory": LatchParameter(
            display_name="Input Directory",
        ),
        "automation_id": LatchParameter(
            display_name="Automation ID",
        ),
    },
)


@workflow(metadata)
def automation_workflow(input_directory: LatchDir, automation_id: str) -> None:
    pass
```

#### Automation Workflow Example

See an [example](automation-usecase.md) of how we create an automation workflow which reads all children of the target directory and kicks off another workflow which runs processing on child directories.

## Creating an Automation

Navigate to [Automations](https://console.latch.bio/automations) tab via **Worfklows** > **Automations** and click on the **Create Automation** button.

Input an **Automation Name** and **Description**.

Next, select a folder where files/folders will be uploaded using the `Select Target` button. Any items uploaded to this folder will trigger the specified workflow.

Finally, select the [automation workflow](#automation-workflow) that you have registered with Latch.

Checkout the [example above](#automation-workflow-example) on how to create and register automation workflows.

![Create Automation Example](../assets/automation/create-automation-example.png)
