# Previewing your Workflow

When iterating on the design of the workflow, it is helpful to preview the user interface locally using `latch preview`.

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

You can click on the button to preview the interface.

![Screenshot of the workflow interface preview page](../assets/ui/previewer.png)
