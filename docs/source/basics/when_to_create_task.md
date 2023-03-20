# When to split up a task into multiple tasks

When building a workflow with multiple tasks, it can be difficult to decide
when to split larger tasks into smaller tasks. Some of the tradeoffs are listed
below to guide this decision:

---

Benefits of splitting a task into mulitple smaller tasks:

* It is easier to manage the dependencies and environments of tasks with less code
* Tasks can be reused between different workflows.
* Each task can be assigned different computing resources.
* Task functions define clear boundaries between steps in a workflow, allowing
for quicker isolation of problems, especially if the tasks are smaller.
* It is easier to retry workflows from the last failed task if tasks are small.
The last succeeded task will be "further along" in the workflow.
* Splitting up tasks creates new nodes in the graph representation of the
  workflow. If each node has one function, may be easier to interpret for biologists.

Downsides of splitting a task into multiple smaller tasks:

* File I/O overhead - files passed between tasks are uploaded to S3 by the
first task and then downloaded by the second task.
with the appropriate resources to be present before it can run and this can take
time.
* Scheduling overhead - each task in a workflow waits for an available machine
with the appropriate resources to be ready before it begins executing. While
this process usually takes under a minute, it can be a significant fraction of
the total runtime for fast-running workflows.
