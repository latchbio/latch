# Previewing your Workflow

Preview the workflow user interface locally using `latch preview` to avoid the lengthy registration process when iterating on the design.

First, verify that you are inside the workflow directory:

```shell-session
$ ls

Dockerfile      reference       wf      version
```

Then, use `latch preview` with the name of your workflow function:

```shell-session
$ latch preview <workflow_function_name>
```

After using `latch preview`, a new button with your workflow name will also be generated on the top right corner of the workflow page.

![Preview](../assets/ui/previewer.png)

You can click on the button to preview the interface.
