# Writing A Workflow

## What is a Workflow?

A workflow on latch is a collection of chained (or not) programs, called tasks, which can be run through the Latch Console in the cloud: [example workflow](https://console.latch.bio/se/crispresso2). A workflow has statically typed inputs (int, string, file, etc) and usually comes with test data. If you choose the test data and hit launch on Latch Console, the workflow executes in the cloud returning any outputs it may have into Latch data.

Example workflow structure (grabbed from the `package_name/__init__.py`) file which is the result of running `latch init`:

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

A Latch workflow is organized by tasks and workflows. Think of a workflow as an overarching umbrella which can execute tasks, but cannot do any computation itself. For example, the following workflow would fail to compile

```
@workflow
def or_wf(
    bool_1: bool, bool_2: bool
) -> str:
    return bool_1 or bool_2
```
This workflow fails because it tries to do computation. A workflow can only pass around inputs and outputs to tasks and return outputs. However, the following workflow is valid

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

The reason why workflows are so limited is that their one job is to determining the order of task execution. This is seen from analyzing how the above workflow executed in the cloud (roughly). First, flyte looks `task` objects. It finds two with the `small_task` decorator and picks up that one has type `f: (bool) -> bool` and the other has type `f: (bool, bool) -> bool`. It then searches for any workflow objects, and finds a single workflow with type `f: (bool, bool) -> bool`. After, it parses the body of the workflow and assigns a to type `bool`, b to type `bool>`, and the return value of type `bool`.

To execute all the tasks and return a value, flyte needs to determine the order in which it makese sense to run the tasks. Luckily, the input for a and b is determined by input variables to the program. These are known values at execution time, so a and b can be calculated as soon as the workflow is launched with inputs `bool_1` and `bool_2`. Once these values are known by flyte, the a and b resolve to values and then the or_task can be executed. Thus by tracking the dependencies of individual tasks, a workflow can sus out the order of execution of tasks. In the above case, the execution DAG looks like such:

			Input--->id(bool_1)--\
				|			      or_task(a,b)--->Output
			    ---->id(bool_2)--/
			  
To run each task, flyte schedules the code inside the task to run on a computer in our backend server. As the tasks complete executing, they pass back the return values to flyte enabling the scheduling of downstream tasks. This process continues until the return statement in the workflow is reached or until there are no more tasks to execute.

## Writing a Workflow

Armed with the above explanation of workflows on latch, let's go through the steps of writing a workflow. These steps can be done in almost any order. It is suggested to run `latch init` first to establish some boilerplate. It should be noted that, while the above examples are rudimentary, the span of what a workflow can be is extreme. On Latch, we have alphafold, built in conda and executed on gpus, numerous nextflow workflows, containing tasks requiring hundreds of gigabytes of memory, and workflows that require terabytes of supporting files. This is all achieveable by you, so let's get into the grit of it.

* Define workflow inputs and outputs and task input and outputs
* Write a workflow description (optional)
* Write input argument descriptions
* Write tasks
* Chain together tasks in workflow
* Write a requirements file (optional)
* Write a Dockerfile (optional)

After these steps, we can register our workflow and execute it from Latch Console. Lets breakdown each step and look at the set of choices we need to make.


## Links for further understanding

* [Flyte Documentation](https://docs.flyte.org/projects/cookbook/en/latest/index.html)