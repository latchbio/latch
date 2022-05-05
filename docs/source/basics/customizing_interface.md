Customizing Your Interface
===

The Latch SDK will dynamically construct interfaces from your python code.

You have a great deal of control over the interface that is constructed from
your python code. Within the docstring of the workflow function, you can
specify:

  * markdown formatted long form documentation
  * sidebar presentation of contact email, repository, social media credits, etc.
  * ordering and grouping of parameters
  * parameter hover-over tooltip descriptions
  * custom parameter display names

## One Line Description

The first line of the workflow function docstring will get rendered in the
sidebar as a brief description of your workflow's functionality.

```
@workflow
def foo(
	...
):
	"""This line is a short workflow description description, displayed in the explore tab

    ...
    """
```

## Markdown Documentation 

The body of the workflow function docstring is where you write long-form
markdown documentation. This markdown will get rendered in the dedicated
documentation tab on your interface.

```
@workflow
def foo(
	...
):
	"""This line is a short workflow description description, displayed in the explore tab

    This line starts the long workflow description in markdown, displayed in the workflow about tab

    Lists
    - item1
    - item2
    - item3

    ## headers

    paragraphs
    
    The line starting in `__metadata__:` signals the end of the long workflow description

    __metadata__:
        ...
    """
```

## Sidebar Customization

After the long workflow description, you have the opportunity to specify
standard workflow metadata that will get rendered in the sidebar. The currently
supported metadata fields are display name, documentation, author (name, email,
github), repository, and license. The only constrained field is license, which
should be an identifier from [spdx](https://spdx.org/licenses/).

```
__metadata__:
    display_name: My Workflow
    documentation: https://github.com/author/my_workflow/README.md
    author:
        name: Workflow Author
        email: licensing@company.com
        github: https://github.com/author
    repository: https://github.com/author/my_workflow
    license:
        id: MIT
```

## Customizing Parameter Presentation

Latch workflows have parameter with names in English, hidden parameters, and
parameters grouped together in sections. A parameter of a certain type, say
string, may display as a line or paragraph, and various parameters only accept a
subset of arguments of said type. For example, a file input may only take files
ending in `.fasta`, `.faa`, or `.fa`.

To facilitate these important restrictions and displays, parameters have
metadata specified either in line with Python's `typing.Annotated` or in the
workflow docstring. Only the display name field is required. Below we show an
example of a workflow with docstring parameter metadata and the equivalent using
`typing.Annotated`. The description language we use to specify this metadata is
`yaml`, as seen in the docstrings below.


**Note**: `typing.Annotated` is necessary when attaching metadata to nested
types. For example, if we have a `List[FlyteFile]` input where each FlyteFile
must be of extension `.fasta`, `.faa`, or `.fa`, typing. Annotated is necessary:

```
...
from flytekit.core.with_metadata import FlyteMetadata
from typing import Annotated
...

@workflow
def test(
	sample_list: List[Annotated[FlyteFile, FlyteMetadata(
		{"rules": [{"regex": "(.fasta|.fa|.faa)$", "message": "Only .fasta, .fa, or .faa extensions are valid"}]}
	)]
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_list:
			__metadata__:
				display_name: "Sample List"
	"""
```

Below are all the available metadata which can be attached to an input

**Display Name** (must be filled out for all parameters, or said parameter will not render in the frontend)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	...
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
		sample_name:
			__metadata__:
				display_name: "Sample Name"
		...
	"""

```
**Hidden Parameters** (moves the parameter into the hidden section)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
		sample_name:
			__metadata__:
				display_name: "Sample Name"
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
	"""

```
**Section Titles** (adds a header before this parameter, indicating a theme amongst following parameters)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
				_tmp:
					section_title: Sample Parameters
		sample_name:
			__metadata__:
				display_name: "Sample Name"
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
					section_title: Output Parameters
	"""

```

**Placeholders** (indication of sample input before user inputs anything)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
				_tmp:
					section_title: Sample Parameters
				appearance:
					placeholder: 10
		sample_name:
			__metadata__:
				display_name: "Sample Name"
				appearance:
					placeholder: "my sample name"
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
					section_title: Output Parameters
				appearance:
					placeholder: "aidan@latch.bio"
	"""

```
**Comments** (a clairification to be displayed next to the parameter name in parenthesis, like this one)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	sample_ph: Optional[float] = None,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
				_tmp:
					section_title: Sample Parameters
				appearance:
					placeholder: 10
		sample_name:
			__metadata__:
				display_name: "Sample Name"
				appearance:
					placeholder: "my sample name"
		sample_ph:
			__metadata__:
				display_name: "pH"
				appearance:
					comment: "1-14 pH scale"
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
					section_title: Output Parameters
				appearance:
					placeholder: "aidan@latch.bio"
	"""

```
**Output** (signals that a FlyteFile or FlyteDirectory is the output location of the workflow, disabling path existence checks on our frontend)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	sample_ph: Optional[float] = None,
	output_dir: FlyteDirectory,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
				_tmp:
					section_title: Sample Parameters
				appearance:
					placeholder: 10
		sample_name:
			__metadata__:
				display_name: "Sample Name"
				appearance:
					placeholder: "my sample name"
		sample_ph:
			__metadata__:
				display_name: "pH"
				appearance:
					comment: "1-14 pH scale"
		output_dir:
			__metadata__:
				display_name: "Output Directory"
				output: true
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
					section_title: Output Parameters
				appearance:
					placeholder: "aidan@latch.bio"
	"""

```
**Batch Table Column** (Signals that a parameter should be displayed in the parameter preview when an input row is collapsed)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	sample_ph: Optional[float] = None,
	output_dir: FlyteDirectory,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
				batch_table_column: true
				_tmp:
					section_title: Sample Parameters
				appearance:
					placeholder: 10
		sample_name:
			__metadata__:
				display_name: "Sample Name"
				batch_table_column: true
				appearance:
					placeholder: "my sample name"
		sample_ph:
			__metadata__:
				display_name: "pH"
				appearance:
					comment: "1-14 pH scale"
		output_dir:
			__metadata__:
				display_name: "Output Directory"
				output: true
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
					section_title: Output Parameters
				appearance:
					placeholder: "aidan@latch.bio"
	"""

```
**[Strings] Type** (defaults to line, options are paragraph and line)

```
@workflow
def test(
	sample_size: int,
	sample_name: str,
	sample_ph: Optional[float] = None,
	sample_description: Optional[str] = None,
	output_dir: FlyteDirectory,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_size:
			__metadata__:
				display_name: "Sample Size"
				batch_table_column: true
				_tmp:
					section_title: Sample Parameters
				appearance:
					placeholder: 10
		sample_name:
			__metadata__:
				display_name: "Sample Name"
				batch_table_column: true
				appearance:
					placeholder: "my sample name"
		sample_ph:
			__metadata__:
				display_name: "pH"
				appearance:
					comment: "1-14 pH scale"
		sample_description:
			__metadata__:
				display_name: "Sample Description"
				appearance:
					type: paragraph
		output_dir:
			__metadata__:
				display_name: "Output Directory"
				output: true
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
					section_title: Output Parameters
				appearance:
					placeholder: "aidan@latch.bio"
	"""

```

**[FlyteFiles and FlyteDirectories] Rule** (regex parameter validation, shows custom error on fail)

```
@workflow
def test(
	sample_file: FlyteFile,
	sample_size: int,
	sample_name: str,
	sample_ph: Optional[float] = None,
	sample_description: Optional[str] = None,
	output_dir: FlyteDirectory,
	...
	email: Optional[str] = None,
):
	"""
	...
	Short description and long description
	...
	
	Args:
		sample_file:
			__metadata__:
				display_name: "Sample File"
				batch_table_column: true
				_tmp:
					section_title: Sample Parameters
				rules:
	                -
	                    regex: "(.fastq.gz|.fastq)$"
	                    message: "Only .fastq or .fastq.gz extensions are valid"
		sample_size:
			__metadata__:
				display_name: "Sample Size"
				batch_table_column: true
				appearance:
					placeholder: 10
		sample_name:
			__metadata__:
				display_name: "Sample Name"
				batch_table_column: true
				appearance:
					placeholder: "my sample name"
		sample_ph:
			__metadata__:
				display_name: "pH"
				appearance:
					comment: "1-14 pH scale"
		sample_description:
			__metadata__:
				display_name: "Sample Description"
				appearance:
					type: paragraph
		output_dir:
			__metadata__:
				display_name: "Output Directory"
				output: true
		...
		email:
			__metadata__:
				display_name: "Email"
				_tmp:
					hidden: true
					section_title: Output Parameters
				appearance:
					placeholder: "aidan@latch.bio"
	"""
```
