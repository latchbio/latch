# Workflow Input and Output


## Possible Workflow Input / Output Types
Workflows consist of a workflow wrapper containing tasks, both the i/o of which is strongly typed. Below are the types supported by latch as expressed in Python types.

```
from typing import Union, Optional
from enum import Enum

```

**Integers**

```
a: int = 10
```

**Floats**

```
a: float = 10.0
```

**Strings**

```
a: str = "bio"
```

**Files**

```
a: FlyteFile = FlyteFile("/root/data.txt")
```

**Directories**

```
a: FlyteDirectory = FlyteDirectory("/root/test_data/")
```

**Enums**

```
class Statistic(Enum):
    min = "min"
    median = "median"
    mean = "mean"
    max = "max"
    
 a: Statistic = Statistic.min
```

Along with the previous basic types, unions and optionals give extra flexibility.

**Unions**

```
a: Union[int, File] = FlyteFile("/root/data.txt")
a = 10
```

**Optionals**

```
a: Optional[int] = 10
a = None
```

Finally, we currently support a single collection type which is List[T].

**Lists**

```
a: List[Optional[int]] = [1, None, 3, 4, None, 6]
```

Each type gets displayed in on the frontend differently in the workflow parameters tab. Try registering a workflow with the various types to see all the different parameter input styles. Additional metadata can be attached in the docstring to customize the display of various types: [parameter metadata](input_parameter_metadata.md).



## FlyteFiles and FlyteDirectories

In flyte, task logic is executed on a computer in the cloud. Thus we need an interface for passing in files and directories from latch data and for returning outputs in a task to latch data. Here we explain the interfaces and how to use them. Additional documentation is found at [FlyteFile](https://docs.flyte.org/projects/flytekit/en/latest/generated/flytekit.types.file.FlyteFile.html) and [FlyteDirectory](https://docs.flyte.org/projects/flytekit/en/latest/generated/flytekit.types.directory.FlyteDirectory.html).

From Latch Console, when you input a file or directory, it is of type FlyteFile or FlyteDirectory. These types specify how to fetch the file / directory to operate on locally. Below is an example of how data is fetched.

```
...
from pathlib import Path
...

@small_Task
def t(fastq: FlyteFile, dir: FlyteDirectory) -> None:
	
	# opening the file will trigger the data to be downloaded locally,
	# and this python filelike object will behave properly
	
	with open(fastq, "w") as f:
		...
		
	# calling Path on a FlyteFile triggers the data to be downloaded locally, 
	# returning the local path object which can be operated on

	fastq_path = Path(fastq)
	
	# calling Path on a FlyteDirectory triggers the data to be downloaded locally,
	# returning the local path object which can be operated on.
	
	dir_path = Path(dir)
```

To get slightly more fine grained for those who are curious, each of these (FlyteFile or FlyteDirectory) is passed in as an object looking roughly like this (field names are different):

```
{
	remote_path (prefix in the case of dir): "s3://bucket/key"
	local_path: None
	downloaded: False
}
```

When the file is accessed, a download is triggered resulting in the object becoming

```
{
	remote_path (prefix in the case of dir): "s3://bucket/key"
	local_path: "/tmp/asdfsanasldfkhafdskfdhad.txt" or "/tmp/asdfsanasldfkhafdskfdhad/"
	downloaded: True
}
```
While inputting FlyteFiles and FlyteDirectories does a good job mimicking normal filelike behavior, outputs require more fine grained manipulation. We recommend the following procedure for outputting data (having a single output directory).

```
...
from pathlib import Path
from typing import Tuple
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile

...

@small_Task
def t(fastq: FlyteFile, dir: FlyteDirectory, output_dir: FlyteDirectory) -> FlyteDirectory
	Logic...
	
	# Created a bunch of output files stored at "/root/myoutputdir"
	
	output_dir.path = "/root/myoutputdir"
	return output_dir
	
```

In this example, we organize all output data into a single directory and set our output_dir FlyteDirectory path to this local directory. Here is a breakdown of what is happening. In Latch Console, you specify where you want your outputs to go in latch data. This gets transformed into a location in the cloud. When you set the path of the output FlyteDirectory and return said FlyteDirectory, flyte uploads your local directory to the cloud location. This data is then synced into latch data and whoever ran your workflow gets the results.

## Note on Default values

When specifying the inputs of your workflow, you can pass in default values. You must pass in default values for any parameter you do not wish the user to set every run. They can always override this value. One notable confusing case is with optionals. Always give optional parameters a default value. See how this is done in the Bactopia example below.

## Note on Predefined Launchplans

When authoring a workflow, it is useful to have default sets of input data for new users to play around with. (TODO(aidan))

```
TODO(aidan) examples
```

## Example 

```
@large_task
def bactopia_tsk(
    fastq_one: Optional[FlyteFile],
    fastq_two: Optional[FlyteFile],
    input_dir: Optional[FlyteDirectory],
    output_dir: FlyteDirectory,
    sample_name: List[Union[str, int]],
    genome_size: Optional[int],
    species: Optional[Species],
    species_genome_size: Optional[SpeciesGenomeSize],
    ask_merlin: bool = False,
    coverage: int = 100,
    hybrid: bool = False,
    skip_logs: bool = False,
    skip_fastq_check: bool = False,
    skip_qc: bool = False,
    skip_error_correction: bool = False,
    no_miniasm: bool = False,
    skip_pseudogene_correction: bool = False,
    skip_adj_correction: bool = False,
    skip_prodigal_tf: bool = False,
    rawproduct: bool = False,
    centre: str = "Bactopia",
    min_contig_length: int = 500,
    amr_plus: bool = False,
) -> FlyteDirectory:
	 # example opening a flytefile
	 with open(Path(fastq_one), "w") as f:
	 	lines = f.readlines() 
	 	
    ... Logic Here ...
    
    local_output_dir = Path("/root/outputs")
  
	# example returning flyte directory
    return FlyteDirectory(
        str(local_output_dir.resolve()),
        remote_directory=_fmt_dir(output_dir.remote_source),
    )


@workflow
def bactopia_wf(
    output_dir: FlyteDirectory,
    sample_name: List[Union[str, int]] = "sample1",
    fastq_one: Optional[FlyteFile] = None,
    fastq_two: Optional[FlyteFile] = None,
    input_dir: Optional[FlyteDirectory] = None,
    genome_size: Optional[int] = None,
    species: Species = Species.none,
    species_genome_size: SpeciesGenomeSize = SpeciesGenomeSize.mash,
    ask_merlin: bool = False,
    coverage: int = 100,
    hybrid: bool = False,
    skip_logs: bool = False,
    skip_fastq_check: bool = False,
    skip_qc: bool = False,
    skip_error_correction: bool = False,
    no_miniasm: bool = False,
    skip_pseudogene_correction: bool = False,
    skip_adj_correction: bool = False,
    skip_prodigal_tf: bool = False,
    rawproduct: bool = False,
    amr_plus: bool = False,
    centre: str = "Bactopia",
    min_contig_length: int = 500,
) -> FlyteDirectory:

    return bactopia_tsk(
        fastq_one=fastq_one,
        fastq_two=fastq_two,
        input_dir=input_dir,
        output_dir=output_dir,
        sample_name=sample_name,
        genome_size=genome_size,
        species=species,
        species_genome_size=species_genome_size,
        ask_merlin=ask_merlin,
        coverage=coverage,
        hybrid=hybrid,
        skip_logs=skip_logs,
        skip_fastq_check=skip_fastq_check,
        skip_qc=skip_qc,
        skip_error_correction=skip_error_correction,
        no_miniasm=no_miniasm,
        skip_pseudogene_correction=skip_pseudogene_correction,
        skip_adj_correction=skip_adj_correction,
        skip_prodigal_tf=skip_prodigal_tf,
        rawproduct=rawproduct,
        centre=centre,
        min_contig_length=min_contig_length,
        amr_plus=amr_plus,
    )

```