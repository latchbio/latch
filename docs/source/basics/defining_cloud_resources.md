# Defining Cloud Resources

When a workflow is executed and tasks are scheduled, the machines needed to run
the task are provisioned automatically and managed for the user until task
completion.

Tasks can be annotated with the resources they are expected to consume (eg. CPU,
RAM, GPU) at runtime and these requests will be fullfilled during the scheduling
process.

---

## Prespecified Task Resource

The Latch SDK currently supports a set of prespecified task resource requests
represented as decorators:

* `small_task`: 2 cpus, 4 gigs of memory, 0 gpus
* `medium_task`: 32 cpus, 128 gigs of memory, 0 gpus
* `large_task`: 96 cpus, 192 gig sof memory, 0 gpus
* `small_gpu_task`: 8 cpus, 32 gigs of memory, 1 gpu (24 gigs of VRAM, 9,216 cuda cores)
* `large_gpu_task`: 31 cpus, 120 gigs of memory, 1 gpu (24 gigs of VRAM, 9,216 cuda cores)

We use the tasks as follows:

```python
from latch import small_task, large_gpu_task

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

## Custom Task Resource

You can also arbitrarily specify task resources using `@custom_task`:
```python
from latch import custom_task

@custom_task(cpu, memory) # cpu: int, memory: int
def my_task(
    ...
):
    ...
```
