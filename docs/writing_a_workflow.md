# Writing A Workflow

## What is a Workflow?

A workflow on Latch is a collection of chained (or not) programs, called tasks, which can be run through the Latch Console in the cloud: [example workflow](https://console.latch.bio/se/crispresso2). ==*<-- manske: this link no work for Safari*== A workflow has statically typed inputs (int, string, file, etc) and usually comes with test data. If you ~~choose the~~ *select* test data and hit launch on Latch Console, the workflow executes in the cloud returning any outputs it may have into Latch data.

==*manske: Think you need a better introduction here, explaining what you're going to be explaining -->*==

Example workflow structure (grabbed from the `package_name/__init__.py`) file which is the result of running `latch init`: ==*<-- manske: is this the right way to start this, is this telling them where to get an example workflow structure? Would rephrase or start from next paragraph*==

==CALL OUT THE DISTINCITON OF WORKFLOW WRAPPPER==

==I WILL MAKES THE DIAMGRAMS==

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

A Latch workflow is organized by tasks and workflows. ==*<-- manske: would include some sort of diagram here?*== Think of a workflow as an overarching umbrella which can execute tasks, but cannot do any computation itself. 

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

Workflows are limited because their one job is to determine the order of a task execution. This is seen from analyzing how the above workflow executed in the cloud (roughly). First, ==manske: capitalize ->== Flyte ==manske: is "looks" a technical term? Or is the grammer wrong (should it be looks for)? ->== looks `task` objects. It finds two with the `small_task` decorator and picks up that one has type `f: (bool) -> bool` and the other has type `f: (bool, bool) -> bool`.  ==<- manske: what does it do with these? It executes them?== It then searches for any workflow objects, and finds a single workflow with type `f: (bool, bool) -> bool`. After, it parses the body of the workflow and assigns a to type `bool`, b to type `bool>`, and the return value of type `bool`. ==???==

To execute all the tasks and return a value, ==F==lyte needs to determine the order in which it makes sense to run the tasks. Luckily, the input for a and b is determined by input variables to the program. These are known values at execution time, so a and b can be calculated as soon as the workflow is launched with inputs `bool_1` and `bool_2`. Once these values are known by ==F==lyte, the a and b resolve to values and then the or_task can be executed. Thus by tracking the dependencies of individual tasks, a workflow can sus out the order of execution of tasks. In the above case, the execution DAG looks like such:

			Input--->id(bool_1)--\
				|			      or_task(a,b)--->Output
			    ---->id(bool_2)--/
			  
To run each task, Flyte schedules the code inside the task to run on a computer in our backend server. As the tasks complete executing, they pass back the return values to flyte enabling the scheduling of downstream tasks. This process continues until the return statement in the workflow is reached or until there are no more tasks to execute. ==manske: Why is this all important, is interesting but you don't explain why I need to know this right now?==

## Writing a Workflow 

==Think this section needs different title==

Armed with the above explanation of workflows on ==L==atch, let's go through the steps of writing a workflow. These steps can be done in almost any order. It is suggested to run `latch init` first to establish some boilerplate. It should be noted that, while the above examples are rudimentary, the span of what a workflow can be is extreme. On Latch, we have ==A==lphafold, built==-==in conda and executed on GPUs, numerous nextflow workflows, containing tasks requiring hundreds of gigabytes of memory, and workflows that require terabytes of supporting files. ==<- Sentence is confusing== This is all achieveable by you, so let's get into the grit of it.

* Define workflow inputs and outputs and task input and outputs: [workflow i/o](workflow_io.md)
* Write a workflow description (optional ==why this optional==): [workflow metadata](workflow_metadata.md)
* Write input parameter descriptions: [parameter metadata](parameter_metadata.md)
* Write tasks: [tasks](tasks.md)
* Chain together tasks in workflow (see above description ==what decription==)
* Write a requirements file (optional): [task dependencies](task_dependencies.md)
* Write a Dockerfile (optional): [task dependencies](task_dependencies.md)
* Write launchplans: [workflow i/o](workflow_io.md)

After these steps, we can register our workflow and execute it from Latch Console. See the documentation ==what documentation== for more information.


## Links

* [Flyte Documentation](https://docs.flyte.org/projects/cookbook/en/latest/index.html)