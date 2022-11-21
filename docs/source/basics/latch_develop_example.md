
# Learning through An Example
To demonstrate how to use `latch develop`, we will walk through an end-to-end flow of testing and debugging an existing variant calling workflow.

## Prerequisites
* Install [Latch](../getting_started/quick_start.md)
* Have a conceptual understanding of how Latch workflows work through reading the [Quickstart](../getting_started/quick_start.md) and [Authoring your own workflow](../getting_started/authoring_your_workflow.md)

## Building a Simple Variant Calling Workflow

In this tutorial, we will be building a simple variant calling workflow. To follow along, you can clone the example code here: 
```
$ git clone https://github.com/hannahle/simple-variant-calling.git

$ cd simple-variant-calling
```

The repository consists of three folders: 
* `buggy-wf`: The workflow with bugs 
* `good-wf`: The final, functional workflow
* `wgs`: Test data for the workflow 

After this example, you will learn how to use `latch develop` to effectively test and debug the `buggy-wf` to arrive at the `good-wf`.

Let's get started!

## Preparing test data 
First, we have to upload our test data folder, `wgs`, to the Latch Platform. 

To do so, you can navigate to [console.latch.bio](https://console.latch.bio), and drag and drop the test data folder on Latch. You should see a spinning wheel which indicates the status of your data upload. 

![Upload](../assets/latch-develop-example/data-upload.png)

Once your data has finished uploading, you can verify whether it exists on Latch by using `latch ls` like so: 
```
$ latch ls

Size Date Modified Name    
   - -             welcome/
   - -             wgs/    
```

## Overview of the variant calling workflow
The data we are working with is part of a long-term evolution experiment by Richard Lenski. The experiment was designed to assess adaptation of _E. coli_. A population was propgated for more than 40,000 generations in a glucose-limited minimal medium that was supplemented with citrate. Sequencing of the populations at differnt time points revealed that that spontaneous citrate-using variant (Cit+) appeared between 31,000 and 31,500 generations, causing an increase in population size and diversity. 

Variant calling is a common workflow used to see
how the population of _E. coli_ changed over time relative to the original population, _E. coli_ strain REL606. Therefore, we will align each of our samples to the _E. coli_ strain REL606 reference genome to determine what differences exist between our reads versus the genome. 

![Upload](../assets/latch-develop-example/variant-calling-wf.png)

Our variant calling pipeline consists of five sequential steps: 
1. Index the reference genome
2. Align reads to reference genome
3. Convert SAM to BAM format 
4. Sort BAM file by coordinates
5. Variant calling

Correspondingly, the Latch SDK workflow contains five tasks (`_build_index`, `align_reads`, `convert_to_bam`, `sort_bam`, and `variant_calling`).

We've provided the code for the five tasks above for you in the `buggy-wf`, which you will now test and debug! 

## Testing and Debugging the Workflow 
* First, enter the `buggy-wf` folder and register it to Latch: 
```
$ ls 
buggy-wf        good-wf         wgs

$ cd buggy-wf

$ latch register --remote .
```

## Defining a test script
* Before testing the workflow end-to-end, it is helpful to run and test each task individually. To do so, you can create a folder called `scripts`, which contain a Python file that calls each of your task functions. 

```
# Folder structure inside buggy-wf
.
├── Dockerfile
├── README.md
├── scripts
│   └── main.py
├── version
└── wf
    ├── __init__.py
    └── __pycache__
        └── __init__.cpython-39.pyc

3 directories, 6 files
```

For example, our `main.py` script can look like: 

```python
# Import task functions from our workflow 
from wf import _build_index, align_reads, convert_to_bam, sort_bam, variant_calling
from latch.types import LatchFile, LatchDir

# Call task function
_build_index(ref_genome = LatchFile("latch:///wgs/ref_genome/ecoli_rel606.fasta")) 
```

* The first line imports all tasks from `wf/__init__.py` to `scripts/main.py`
* The second line imports the appropriate Latch types
* The third line calls the task function to index the reference genome, `_build_index`. 

![Copy](../assets/latch-develop-example/copy.png)
* To use a file on Latch as test data to the task, navigate to Latch Console, click on that file and copy the path using the "Copy" button on the sidebar. After copying the path, you can prefix it with `latch://` to specify that it is a file on Latch, and pass the whole string as a parameter to `LatchFile`. 

## Calling the test script 
* Inside `buggy-wf`, start a devleopment session with: 
```
$ latch develop .
```
Output: 
```
Copying your local changes...
Could not find /Users/hannahle/Documents/GitHub/simple-variant-calling/buggy-wf/data - skipping
Done.
Successfully connected to remote instance.
Pulling 6064_buggy-wf, this will only take a moment... 
Image successfully pulled.
>>>
```

* To run the test script, type: 
```
>>> run-script scripts/main.py
```
Output: 
```
Syncing your local changes... 
Could not find /Users/hannahle/Documents/GitHub/simple-variant-calling/buggy-wf/data - skipping
Finished syncing. Beginning execution and streaming logs:
Finished downloading ecoli_rel606.fasta

====================
2022-11-17 00:09:13,104 flytekit ERROR Exception occured when executing task: [Errno 2] No such file or directory: 'bwa'
====================
```
* The logs tell us that there is no file or directory called `bwa`. One potential reason why is we might not have installed the binary `bwa` correctly. 

* To check this, let's open up a shell and check if the `bwa` command exists:
```
>>> shell

account-4034-development@ip-10-0-11-243:~$ 
```
which opens up an interactive bash session. 
```
account-4034-development@ip-10-0-11-243:~$ bwa
bash: bwa: command not found
```
* Indeed, our `bwa` binary was not installed properly! 
* Checking the Dockerfile, we noticed that the installation instruction for `bwa` was commented out. Let's uncomment it.
```Dockerfile
...
RUN apt install bwa
...
```

* Because we made a modification to our Dockerfile, we have to rebuild the environment and enter a new development session to load in the newest changes. 
* Exit your current development session: 
```
account-4034-development@ip-10-0-11-243:~$ exit
>>> exit
Exiting local development session
```
* Re-register your workflow with the new Docker image
```
$ latch register --remote .
```
* Once your workflow finishes registering, enter a new development session:
```
$ latch develop .
```
* Now, re-run the test script: 
```
>>> run-script scripts/main.py
```
Your script should now run successfully!

**Where are my outputs?**
* Let's inspect the return statement of the `_build_index` task inside `wf/__init__.py`:
```python
@small_task
def _build_index(ref_genome: LatchFile = LatchFile("latch:///wgs/ref_genome/ecoli_rel606.fasta")) -> LatchDir: 
    _bwa_cmd = [
        "bwa", 
        "index",
        ref_genome.local_path
    ]
    subprocess.run(_bwa_cmd)
    output = os.path.dirname(os.path.abspath(ref_genome.local_path))

    return LatchDir(output, "latch:///wgs/ref_genome")
```
* We can see the task is returning a `LatchDir` with the filepath `latch:///wgs/ref_genome`, which indicates that the output files are located inside the `/wgs/ref_genome` folder on Latch!

![Outputs](../assets/latch-develop-example/ref_genome.png)

## Debugging Subsequent Tasks
* Similarly to how we tested our first task, we can also call the second task from `scripts/main.py` like so: 
```python
from wf import _build_index, align_reads, convert_to_bam, sort_bam, variant_calling
from latch.types import LatchFile, LatchDir

ref_genome_dir = _build_index(ref_genome = LatchFile("latch:///wgs/ref_genome/ecoli_rel606.fasta")) 

align_reads(ref_genome_dir = ref_genome_dir, read1 = LatchFile("latch:///wgs/trimmed_fastqs/SRR2584863_1.trim.sub.fastq"), read2 = LatchFile("latch:///wgs/trimmed_fastqs/SRR2584863_2.trim.sub.fastq"))
```
* Here, we are passing the output of the first task, `latch:///wgs/ref_genome`, as the input to the second task. 
* Run the test script: 
```
>>> run-script scripts/main.py

Syncing your local changes... 
Could not find /Users/hannahle/Documents/GitHub/simple-variant-calling/buggy-wf/data - skipping
Finished syncing. Beginning execution and streaming logs:
Finished downloading ecoli_rel606.fasta.amb
Finished downloading ecoli_rel606.fasta.ann
Finished downloading ecoli_rel606.fasta.pac
Finished downloading ecoli_rel606.fasta.sa
Finished downloading ecoli_rel606.fasta
Finished downloading ecoli_rel606.fasta.bwt
['/tmp/flyte-4jp7wtbm/sandbox/local_flytekit/638a02102804a22fa669f18a715b580a/ecoli_rel606.fasta']
====================
2022-11-17 00:42:29,046 flytekit ERROR Exception occured when executing task: Failed to get data from latch:///wgs/trimmed_fastqs/SRR2584863_1.trim.sub.fastq to /tmp/flyte-4jp7wtbm/sandbox/local_flytekit/3b8d7234fe9c7d3e37ec78af9c592208/SRR2584863_1.trim.sub.fastq (recursive=False).

Original exception: failed to get presigned url for `latch:///wgs/trimmed_fastqs/SRR2584863_1.trim.sub.fastq`
====================
```

* The error tells us that there is no file called `/wgs/trimmed_fastqs/SRR2584863_1.trim.sub.fastq` on Latch. Referencing the actual file paths of the trimmed FastQs, we can see that the filepath is indeeed wrong, with the correct filepath being `/wgs/data/SRR2584863_1.trim.sub.fastq`. 
* We can make this modification to our test script and re-run the task
```
>>> run-script scripts/main.py 
```
* The task now outputs the results to the folder `/results` on Latch!

![Aligned](../assets/latch-develop-example/aligned.png)

## Exercise
As an exercise, you are welcome to continue and debug the final three tasks of the whole workflow. The solution of a working workflow is provided in the `good-wf` folder for reference.

---
## Key Takeaways
From the tutorial, you learned:
* How to open a development session with `latch develop`
* How to open an interactive bash for quick debugging of commands with `shell`
* How to call a task and run a test script with `run-script`