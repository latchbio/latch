# Writing A Workflow

## What Is A Workflow?

A workflow on Latch is a collection of chained (or not) programs, called tasks, which are run through the Latch Console in the cloud: [example workflow](https://console.latch.bio/se/crispresso2). A workflow has statically typed inputs (int, string, file, etc) and usually comes with test data. If you choose the test data and hit launch on Latch Console, the workflow executes in the cloud returning any outputs it may have into Latch data. The span of what a workflow can do is massive -- for example we have Alphafold2, which uses conda and executes on GPUs, and nf_core sarek, which require hundreds of gigabytes of memory. Writing workflows as powerful as these is achieveable by you, so let's get into the grit of it.

## How Do I Write A Workflow?

The steps of writing a workflow are deliniated below and can be done in almost any order. To initalize some boilerplate, run `latch init workfow_name`. To gain further understanding of how workflows "work", check out the [how workflows "work"](#how-workflows-work) section.

* Define workflow inputs and outputs and task input and outputs: [workflow i/o](workflow_io.md)
* Write a workflow description: [workflow metadata](workflow_metadata.md)
* Write input parameter descriptions: [parameter metadata](parameter_metadata.md)
* Write tasks: [tasks](tasks.md)
* Chain together tasks in workflow (see chained tasks in [how workflows "work"](#how-workflows-work))
* Write a requirements file (optional): [task dependencies](task_dependencies.md)
* Write a Dockerfile (optional): [task dependencies](task_dependencies.md)
* Write launchplans: [workflow i/o](workflow_io.md)

After these steps, we can register our workflow and execute it from Latch Console. For how to register, see the [docs](register.md).

## How Workflows Work

```
@small_task
def or_task(
    bool_1: bool, bool_2: bool
) -> bool:
    return bool_1 or bool_2


@workflow
def or_wf(
    bool_1: bool, bool_2: bool
) -> str:
    return or_task(
        bool_1=bool_1,
        bool_2=bool_2
    )
```

A Latch workflow is organized by tasks and workflows. Think of a workflow as an overarching umbrella which can execute tasks, but cannot do any computation itself. 

For example, the following workflow would fail to compile:

```
@workflow
def or_wf(
    bool_1: bool, bool_2: bool
) -> str:
    return bool_1 or bool_2
```
This workflow fails since it tries to do computation. A workflow can only pass around inputs and outputs to tasks and return outputs. However, the following workflow is valid

```
@small_task
def id(
    input: bool
) -> bool:
    return input

@small_task
def or_task(
    bool_1: bool, bool_2: bool
) -> bool:
    return bool_1 or bool_2

@workflow
def or_wf(
    bool_1: bool, bool_2: bool
) -> str:
    a = id(input=bool_1)
    b = id(input=bool_2)
    return or_task(bool_1=a, bool_2=b)
```

Workflows are limited because their one job is to determine the order of a task execution. This is seen from analyzing how the above workflow executed in the cloud (roughly). First, Flyte looks for `task` objects. It finds two with the `small_task` decorator and picks up that one has type `f: (bool) -> bool` and the other has type `f: (bool, bool) -> bool`. It then searches for any workflow objects, and finds a single workflow with type `f: (bool, bool) -> bool`. After, it parses the body of the workflow and assigns a to type `bool`, b to type `bool>`, and the return value of type `bool`. It then validates that the input and output types for each task match what is being given (they do), and proceeds to execute.

To execute all the tasks and return a value, Flyte needs to determine the order in which it makes sense to run the tasks. Luckily, the input for a and b is determined by input variables to the program. These are known values at execution time, so a and b can be calculated as soon as the workflow is launched with inputs `bool_1` and `bool_2`. Once these values are known by Flyte, the a and b resolve to values and then the or_task can be executed. Thus by tracking the dependencies of individual tasks, a workflow can sus out the order of execution of tasks. In the above case, the execution DAG looks like such:

			Input--->id(bool_1)--\
				|			      or_task(a,b)--->Output
			    ---->id(bool_2)--/
			  
To run each task, Flyte schedules the code inside the task to run on a computer in our backend server. As the tasks complete executing, they pass back the return values to flyte enabling the scheduling of downstream tasks. This process continues until the return statement in the workflow is reached or until there are no more tasks to execute.


## Links

* [Flyte Documentation](https://docs.flyte.org/projects/cookbook/en/latest/index.html)