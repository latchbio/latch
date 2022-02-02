# `latch register`

### Turns a workflow directory (from `latch init`) into a cloud native workflow executable from Latch Console. See [writing a workflow](writing_a_workflow.md) to get ready for this step.

## Syntax

### `latch register path_to_workflow_directory`

The first argument specifies the local path in which to look for workflow objects. Inside the local path should be a single directory containing `__init__.py`, any helper python files, and `version.txt`.

## Optionals

`--dockerfile` -- used as the execution environment for your workflow (see [writing a workflow](writing_a_workflow.md) for more details).

`--requirements` -- pip installs these requirements into your workflow execution environment if dockerfile provided or the default environment if no dockerfile provided.