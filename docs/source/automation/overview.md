# Workflow Automations

Note: This document is a work in progress and is subject to change.

## Description

Automations allow you to automatically run workflows on top of folders in Latch Data when triggered by specific events such as when files are added to folders or on a regular interval. Automations consist of a [*trigger*](#trigger) and an [*automation workflow*](#automation-workflow).

Additionally, you can pause and resume automations by toggling status radio on the sidebar.

## Triggers

Automation trigger specifies the conditions to run the automation: child got added to the target directory, interval expired, etc.

<!-- It allows you to specify a target directory to watch, the [_event_](#trigger-event-types) which kicks off a workflow, and a [_timer_](#trigger-timer). -->

### Available Trigger Types

#### Data Added

This trigger type runs [automation workflow](#automation-workflow) if a new child has been added to the target directory at any depth. Automation will not run if the child has been modified or deleted.

_Trigger Parameters_:

- `Follow-up Update Period`: this is the wait period after the last trigger event after which the workflow will run.\
For example, if the timer is 10 minutes and the trigger event is `Child Added`, the automation will wait 10 minutes after a child has been added to the target directory and then run automation workflow.
- `Input Target`: trigger will watch this target Ldata directory and will be activated when a child is added at any depth.

_Example_: automation with `Data Added` trigger with `Input Target` directory as `/test` and `Follow-up Update Period` as 10 minutes, will run the [automation workflow](#automation-workflow) 10 minutes after the last child is added at any depth to `/test` directory in Latch Data.

#### Interval
This trigger type runs [automation workflow](#automation-workflow) on a regular interval specified by the user.

_Trigger Parameters_:

- `Interval`: trigger will be activated and will run [automation workflow](#automation-workflow) at a regular interval.

_Example_: automation with `Interval` trigger with `Interval` as `1 hour` will run the [automation workflow](#automation-workflow) hourly.

## Automation Workflow

This is the [workflow](../basics/what_is_a_workflow.md) that will run whenever the automation has been [triggered](#trigger).

#### Usage Note:

- When using [`Data Added`](#data-added) trigger, automation workflow has to have `input_directory` as the only parameter. If your workflow has different parameters automation will fail to start it.

    _Required Workflow Definition_:
    ```python
    # __init__.py

    from latch.resources.workflow import workflow
    from latch.types.directory import LatchDir, LatchOutputDir
    from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter
    from wf.automation import automation_task

    metadata = LatchMetadata(
        # MODIFY NAMING METADATA BELOW
        display_name="Workflow Name",
        author=LatchAuthor(
            name="Your Name Here",
        ),
        # MODIFY NAMING METADATA ABOVE
        # IMPORTANT: these exact parameters are required for the workflow to work with automations
        parameters={
            "input_directory": LatchParameter(
                display_name="Input Directory",
            )
        },
    )


    @workflow(metadata)
    def automation_workflow(input_directory: LatchDir) -> None:
        pass
    ```

- When using [`Interval`](#interval) trigger, automation workflow has to have no parameters. If your workflow has any parameters automation will fail to start it.

    _Required Workflow Definition_
    ```python
    # __init__.py

    from latch.resources.workflow import workflow
    from latch.types.directory import LatchDir, LatchOutputDir
    from latch.types.metadata import LatchAuthor, LatchMetadata, LatchParameter
    from wf.automation import automation_task

    metadata = LatchMetadata(
        # MODIFY NAMING METADATA BELOW
        display_name="Workflow Name",
        author=LatchAuthor(
            name="Your Name Here",
        ),
        # IMPORTANT: these exact parameters are required for the workflow to work with automations
        parameters={
        },
    )


    @workflow(metadata)
    def automation_workflow() -> None:
        pass
    ```

    In case you need more parameters to pass your workflow, we suggest to hard-code them into your workflow while we are working on adding parameter support for automations.

See an [example](automation-usecase.md) of how we create an automation workflow with `Data Added` trigger which reads all children of the target directory and kicks off another workflow which runs processing on child directories.

## Creating an Automation

1. Register automation workflow with Latch. See [Usage Note](#usage-note) to make sure that your workflow can be run by automations.

2. Navigate to [Automations](https://console.latch.bio/automations) tab via **Worfklows** > **Automations** and click on the **Create Automation** button.

    1. Input an **Automation Name** and **Description**.

    2. Select the `Event Type`. Refer to the [Available Trigger Types](#available-trigger-types) for explanation of trigger behaviors.

    3. Specify `Follow-up Update Period` or `Interval` depending on the type of the trigger you have selected.

    4. (For [`Data Added`](#data-added) trigger) select a folder where files/folders will be uploaded using the `Select Target` button. Any items uploaded to this folder will trigger the specified workflow.

    5. Select the [automation workflow](#automation-workflow) that you have just registered with Latch.

Checkout an [example](automation-usecase.md) on how to create and register automation workflows with `Data Added` triggers.

![Create Automation Example](../assets/automation/create-automation-example.png)
