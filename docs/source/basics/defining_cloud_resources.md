# Defining Cloud Resources

When a workflow is executed and tasks are scheduled, the machines needed to run
the task are provisioned automatically and managed for the user until task
completion.

Tasks can be annotated with the resources they are expected to consume (eg. CPU,
RAM, GPU) at runtime and these requests will be fullfilled during the scheduling
process.

---

The Latch SDK currently supports a set of prespecified task resource requests
represented as decorators:

* `small_task`: 2 cpus, 4 gigs of memory, 0 gpus
* `medium_task`: 8 cpus, 32 gigs of memory, 0 gpus
* `large_task`: 31 cpus, 120 gigs of memory, 0 gpus
* `medium_gpu_task`: 7 cpus, 30 gigs of memory, 1 gpu (16 gigs of VRAM, 2,560 cuda cores)
* `large_gpu_task`: 31 cpus, 120 gigs of memory, 1 gpu (24 gigs of VRAM, 9,216 cuda cores)

We use the tasks as follows:

```python
from latch import small_task

@small_task
def my_task(
    ...
):
    ...

@large_gpu_task
def inference(
    ...
):
    ...
```
