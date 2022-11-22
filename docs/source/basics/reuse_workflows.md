# Reusing Workflows

With the Latch SDK, there are two ways you can reuse workflows via **subworkflows** and **workflow references**. These constructs allow for arbitrary composition of workflows within each other, enabling great organizational flexibility as well as reducing code duplication.

## Subworkflow

To create a subworkflow, simply create two functions with the `@workflow` decorator and call one inside the other, as below:

```python
@small_task
def this_is_a_sub_wf_task(a: int) -> int:
	print(“This is a sub-workflow”)
	return a + 1
  

@workflow
def this_is_a_sub_wf(b: int) -> int:
	return this_is_a_sub_wf_task(a=b)
  

@workflow
def this_is_a_top_level_wf(c: int) -> int:
	return this_is_a_sub_wf(b=c)
```

To view a full code example of how subworkflows are used to compose a real-world metagnomics pipeline, visit our tutorial [here](../tutorials/metamage.md).

## Workflow Reference
A reference workflow is distinct from a subworkflow in that a reference workflow is a reference to an existing workflow, meaning workflows are reusable in other workflows without duplicating code. 

To create a workflow reference, simply annotate an empty function with the `@workflow_reference` decorator as below.

```python
# import statement

@workflow_reference(
	name=“wf.__init__.assemble_and_sort”,
	version=“0.0.1”,
)

def assemble_and_sort(read1: LatchFile, read2: LatchFile):
	...
```

### Note to Kenny: 
- We should define criteria that needs to be met to successfully use workflow_reference. For example: 
+ workflow version must exist
+ etc etc