# Workflow Input and Output

==Should have a better introduction...==

## Possible Workflow Input / Output Types
Workflows consist of a workflow wrapper ==manske: what is a wrapper?== containing tasks, both the i/o of which is strongly typed. Below are the types supported by Latch as expressed in Python types.

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

In Flyte, task logic is executed on a computer in the cloud. Thus we need an interface for passing in files and directories from Latch data and for returning outputs in a task to Latch data. Here we explain the interfaces and how to use them. 

Additional documentation is found at [FlyteFile](https://docs.Flyte.org/projects/Flytekit/en/latest/generated/Flytekit.types.file.FlyteFile.html) and [FlyteDirectory](https://docs.Flyte.org/projects/Flytekit/en/latest/generated/Flytekit.types.directory.FlyteDirectory.html).

From Latch Console, when you input a file or directory, it is of type FlyteFile or FlyteDirectory respectively. These types specify how to fetch the file/directory to operate on locally. Below is an example of how data is fetched.

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
While inputting FlyteFiles and FlyteDirectories does a good job mimicking normal file like behavior, outputs require more fine grained manipulation. We recommend the following procedure for outputting data: ==don't love the flow here so played around with it==


```
...
from pathlib import Path
from typing import Tuple
from Flytekit.types.directory import FlyteDirectory
from Flytekit.types.file import FlyteFile
...

@small_Task
def t(fastq: FlyteFile, dir: FlyteDirectory, output_dir: FlyteDirectory) -> FlyteDirectory
	Logic...
	
	# Created a bunch of output files stored at "/root/myoutputdir"
	
	output_dir.path = "/root/myoutputdir"
	return output_dir
	
```


Set a single directory to organize all output data into and setting the output_dir FlyteDirectory path to this local directory. 

Here is a breakdown of this procedure. In Latch Console, specify where you want your outputs to go in Latch data. This gets transformed into a location in the cloud. When you set the path of the output as FlyteDirectory and return said FlyteDirectory, Flyte will upload your local directory to the cloud location. This data is then synced into Latch data and whoever ran it gets the results ==when the work completes??==.

==Some details on the why maybe?==

## Setting Default Values

When specifying the inputs of your workflow, you can pass in default values. You must pass in default values for any parameter you do not wish the user to set every run. They can always override this value. One notable confusing case is with optionals. Always give optional parameters a default value. See how this is done in the Bactopia example below. ==Why not show example here??==

## Launchplans (Setting Test Data for Users)

When authoring a workflow, it is useful to have ~~default~~ ==test== sets of input data for new users to play around with. In Flyte, we use launchplans to capture this concept. In the launchplan, you must provide values for any non-optional parameter without a default value. ==Would wanna see the anatomy of a plan??==

```
from Flytekit import LaunchPlan, task, workflow
...
@large_task
def bactopia_tsk(
	fastq_one: FlyteFile,
    fastq_two: FlyteFile,
    sample_name: str,
    output_dir: FlyteDirectory,
    coverage: int = 100,
    species: Optional[Species] = None,
):
	...

@workflow
def bactopia_wf(
	fastq_one: FlyteFile,
    fastq_two: FlyteFile,
    sample_name: str,
    output_dir: FlyteDirectory,
    coverage: int = 100,
    species: Optional[Species] = None,
):
	return bactopia_tsk(
		...
	)

LaunchPlan.create(
   "bactopia_wf.Paired Reads",
    bactopia_wf,
    default_inputs={
        "fastq_one": FlyteFile(
            "s3://Latch-public/welcome/bactopia/SRX4563634_R1.fastq.gz"
        ),
        "fastq_two": FlyteFile(
            "s3://Latch-public/welcome/bactopia/SRX4563634_R2.fastq.gz"
        ),
        "sample_name": "SRX4563634",
        "output_dir": FlyteDirectory("Latch://bactopia_paired_results/"),
    },
)

LaunchPlan.create(
   "bactopia_wf.Paired Reads With Optionals",
    bactopia_wf,
    default_inputs={
        "fastq_one": FlyteFile(
            "s3://Latch-public/welcome/bactopia/SRX4563634_R1.fastq.gz"
        ),
        "fastq_two": FlyteFile(
            "s3://Latch-public/welcome/bactopia/SRX4563634_R2.fastq.gz"
        ),
        "sample_name": "SRX4563634",
        "output_dir": FlyteDirectory("Latch://bactopia_paired_results/"),
        "coverage": 120,
        "species": Species.staphylococcus_aureus,
    },
)
```

## Example

ADD IN BREIF DESCRIPT + clink to Bactopia

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
	 # example opening a Flytefile
	 with open(Path(fastq_one), "w") as f:
	 	lines = f.readlines() 
	 	
    ... Logic Here ...
    
    local_output_dir = Path("/root/outputs")
  
	# example returning Flyte directory
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

==Where do I go next :( ?======