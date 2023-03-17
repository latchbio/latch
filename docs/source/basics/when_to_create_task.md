# When to create a Task?

A workflow is an analysis that takes in some input, processes it in one or more
steps and produces some output.

---

Reasons to create a new task:

* It is easier to manage dependencies / containers for smaller tasks.
* Tasks can be reused between different workflows.
* Tasks can be assigned different computing resources.
* It is easier to isolate and debug failed tasks if tasks are small
* It is easier to retry workflows from the last failed task if tasks are small.
The last succeeded task will be "further along" in the workflow.
* Splitting up tasks creates new nodes in the graph representation of the
  workflow. This may be easier to interpret for biologists.

Reasons to not create a new task:

* File I/O overhead - files that are passed between tasks need to be first
  uploaded to S3 and downloaded again before they can be used.
* Scheduling overhead - every new task needs to wait for an available machine
with the appropriate resources to be present before it can run and this can take
time.
* Development overhead - it takes time to split up one python function into two
python functions. Lots of intermediate parameters and complex intermediate
values can make this tedious.
