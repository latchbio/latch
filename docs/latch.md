# Latch CLI: The Official Latch Command-line Interface

Latch CLI allows users to interface with Latch Console from the terminal. It currently supports uploading files to latch and registering custom workflows.


## Install
Latch CLI is available on pypi and can be pip installed using `pip install latch`.

## Authorization

### `latch login`

Authenticates with Latch Console and grants an authorization token stored at `~/.latch/token`. Necessary for any of the following commands.

## Commands

### [`latch cp`](cp.md) - copies local files into Latch Console

### [`latch init`](init.md) - generates a workflow template in your current directory

### [`latch register`](register.md) - uploads a workflow from your current directory

## Explanations

### [Writing a Latch Workflow](writing_a_workflow.md)

## Acknowledgements

Latch owes the [flyte](https://flyte.org) project a large amount of credit for putting together a rockstar workflow orchestration platform, allowing the creation of strongly typed workflows (arbitrary bioinformatics pipelines) and the running of said workflows at immense scale. Special shoutout to Ketan Umare and Haytham Abuelfutuh for supporting our integration with flyte and 
 
