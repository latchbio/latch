# Adding Documentation to your Workflow

While most of the metadata of a workflow will be encapsulated in a LatchMetadata object, we still require a docstring in the body of the workflow function which specifies both a short and long-form description.

## One Line Description

The first line of the workflow function docstring will get rendered in the sidebar of the workflow and the workflow explore tab as a brief description of your workflow's functionality. Think of this as summarizing the entirety of your workflow's significance into a single line.

We recommend limiting your workflow description to one sentence, as longer descriptions are only partially rendered on the Workflows page.

```python
from latch import workflow

@workflow
def rnaseq(
    ...
):
    """Perform alignment and quantification on Bulk RNA-Sequencing reads.

    ...
    """
    ...
```

![Short Description](../assets/ui/one-line-description.png)

## Long Form Description

The body of the workflow function docstring supports [full GitHub-flavored markdown](https://github.github.com/gfm/) and [KaTeX, a subset of LaTeX for mathematical typesetting](https://katex.org/docs/supported.html).

Here you can write long-form documentation, which will get rendered in the About" tab on the workflow interface.

```python
from latch import workflow

@workflow
def rnaseq(
    ...
):
    """Perform alignment and quantification on Bulk RNA-Sequencing reads

    Bulk RNA-Seq (Alignment and Quantification)
    ----
    This workflow will produce gene and transcript counts from bulk RNA-seq
    sample reads.

    # Disclaimer

    This workflow assumes that your sequencing reads were derived from *short-read
    cDNA seqeuncing* ...

    # Brief Summary of RNA-seq

    This workflow ingests short-read sequencing files (in FastQ format) that came
    from the following sequence of steps[^1]:
      - RNA extraction from sample
      - cDNA synthesis from extracted RNA
      - adaptor ligation / library prep
      - (likely) PCR amplification of library
      - sequencing of library
    You will likely end up with one or more FastQ files from this process that hold
    the sequencing reads in raw text form. This will be the starting point of our
    workflow.
    ...
    """
```

![Long Description](../assets/ui/long-form-description.png)
