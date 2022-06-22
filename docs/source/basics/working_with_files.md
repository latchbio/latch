# Working with Files

When working with bioinformatics workflows, we are often passing around large
files or directories between our tasks. These files are usually located in
cloud object stores and are copied to the file systems of the machines
on which the task is scheduled.

The Latch SDK provides a convenient means of referencing files or directories
within task functions without worrying about how or when the passed file objects
are copied to the task's machine at execution.

---

Let's look at an example.

```python
from pathlib import Path


@small_task
def foo(fastq: LatchFile, output_dir: LatchDir) -> (LatchFile, LatchDir):

    # When you pass parameter values of type LatchFile or LatchDir, the file will
    # be automatically downloaded on whatever machine the task is scheduled on.

    # Passing the parameter value to a python Path object and resolving it is a
    # common pattern to retrieve the full path of the file on the local filesystem for
    # downstream use.

    local_fastq = Path(fastq).resolve()
    local_output_dir = Path(dir).resolve()

    # It's now easy to reference the contents of the file in a subprocessed
    # program. Notice how we're 'placing' outputs in a directory we will return.
    subprocess.call(["myprogram", "analyze", "local_fastq", "-o", str(local_output_dir)])

    # We can also simply read out the contents of the file as we would normally.
    with open(local_fastq) as fq:
      reads = fq.read()

    # Lets make a new file on this machine and return it along with the results of
    # the previous subprocess.
    with open("/root/foobar", "w") as fb:
      fb.write("fizzbuzz")

    # Notice when we return, we must specify *two* values - a local path and a
    # remote path. We need to know where the file is coming from and where it's
    # going. We'll discuss the latch URL scheme in a moment, but just understand
    # it will go back in your filesystem on the LatchBio console for now.
    return LatchFile("/root/foobar", "latch:///foobar.txt"), LatchDir(local_output_dir, output_dir.remote_path)
```

## Local Paths and Remote Paths

In the majority of cases, we can just use a value annotated with `LatchFile` or
`LatchDir` and expect it to yield a file handler pointing to a local file. This
gives good synergy with `Path` or `open` as we've seen above.

However, it is important to understand that these values _really_ have both a
local and remote path associated with them.

```python
# latch/types/directory.py

    @property
    def local_path(self) -> str:
        """File path local to the environment executing the task."""
        return self._path

    @property
    def remote_path(self) -> Optional[str]:
        """A url referencing in object in LatchData or s3."""
        return self._remote_directory

```

`local_path` will always be the absolute path on the task's machine where the
file has been copied.  `remote_path` will be a remote object URL with `s3` or
`latch` as its host.

There are cases when we would want
to access these `local_path` and `remote_path` attributes directly:

* Specifying the remote destination of a returned directory (eg. in the above return statement).
* Manually fetching additional files from s3 similar to a passed file's remote source.
* Using the Latch SDK to list other files similar to a passed file (eg. `latch ls latch:///foo`)

## Using Globs to Move Groups of Files

Often times logic is needed to move groups of files together based on a shared
pattern. For instance, you may wish to return all files that end with a
`fastq.gz` extension after a
[trimming](https://bmcbioinformatics.biomedcentral.com/articles/10.1186/s12859-016-1069-7#:~:text=Trimming%20of%20adapter%20sequences%20from,previously%20published%20adapter%20trimming%20tools.)
task has been run.

To do this in the SDK, you can leverage the `file_glob` function to construct
lists of `LachFile`s defined by a pattern.

The class of allowed patterns are defined as
[globs](https://en.wikipedia.org/wiki/Glob_(programming)). It is likely you've
already used globs in the terminal by using wildcard characters in common
commands, eg. `ls *.txt`.

The second argument must be a valid latch URL pointing to a directory. This will
be the remote location of returned `LatchFile` constructed with this utility.

In this example, all files ending with `.fastq.gz` in the working directory of
the task will be returned to the `latch:///fastqc_outputs` directory:

```python
@small_task
def task():

    ...

    return file_glob("*.fastq.gz", "latch:///fastqc_outputs")
```

### `latch:///` URLs

Recall that URLs (Uniform Resource Locators) describe the location of an object
on the internet.

A simplified representation of a URL string syntax can be denoted as:

```text
scheme://<host>/<url-path>
```

Where `https://google.com` and `s3://my-bucket/dna.fa` are both valid descriptions of
objects, a webpage or a fasta file.

When referencing files stored within LatchBio's _managed filesystem_ (called
LatchData) we must use the `latch` scheme to appropriately resolve objects to
the appropriate account.

For instance, `latch:///foo.txt` might meant two entirely different things in
the context of two different accounts. The resolution to retrieve the correct
object occurs based on the user that executed the workflow,

Some examples of valid latch URLs referencing objects in a user's filesystem:

* `latch:///guide_design/off_targets.csv`
* `latch:///foo.txt`

Note the three slashes. This is not accidental, but is in strict accordance with
the [URL specification](https://www.ietf.org/rfc/rfc1738.txt) as there is no
real user-facing "host" for latch objects.

### Shared `latch` URLs

Paths that are shared amongst accounts will bear the `latch://shared/<path>`
syntax.
