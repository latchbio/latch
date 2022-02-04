# Tasks
Tasks are the heart of any Flyte workflow where all the computation happens. Let's go over the anatomy of a task.

## Resources

To declare a task, you need to use one of the latch task decorators. These decorators declare a task with some amount of cpu, memory, and gpus. Below are the six available task decorators and the lower bound on resources they have access to:

* `small_task`: 2 cpus, 4 gigs of memory, 0 gpus
* `medium_task`: 8 cpus, 32 gigs of memory, 0 gpus
* `large_task`: 31 cpus, 120 gigs of memory, 0 gpus
* `medium_gpu_task`: 7 cpus, 30 gigs of memory, 1 gpu (16 gigs of VRAM, 2,560 cuda cores)
* `large_gpu_task`: 31 cpus, 120 gigs of memory, 1 gpu (24 gigs of VRAM, 9,216 cuda cores)

```
from latch.resources.tasks import small_task

@small_task
def my_task(
	...
):
	...
```

## Computation

Almost anything. Call libraries, use conda environments, write algorithms, etc. The only limitation between a flyte workflow and running your code on your linux computer is that we restrict your access to `/dev/` and the networking stack. For example, you cannot create mounts using `/dev/fuse` (so mounts are generally off limits) and you do not have admin access to the networking stack on the host machine as your task execution does not have root access. Here are some examples of flyte tasks to get started, or you can just write your task as it is Python code which does not require much further explanation.

*	TODO(examples)


## Requirements

To ensure that you have all the dependencies you need, you can provide a `requirements.txt` or a Dockerfile (see [task dependencies](task_dependencies.md)).