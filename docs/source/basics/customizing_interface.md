# Customizing Your Interface

The Latch SDK will dynamically construct interfaces from your python code.

You have a great deal of control over the constructed interface.  Within the
docstring of the workflow function, you can specify:

* markdown formatted long form documentation
* sidebar presentation of contact email, repository, social media links, etc.
* the ordering and grouping of parameters
* parameter tooltip descriptions
* parameter display names

---

## One Line Description

The first line of the workflow function docstring will get rendered in the
sidebar and the workflow expore tab as a brief description of your workflow's functionality.

```python
@workflow
def foo(
    ...
):
    """This line is a short workflow description, displayed in the explore tab and sidebar.

    ...
    """
    ...
```

## Markdown Documentation

The body of the workflow function docstring is where you write long-form
markdown documentation. This markdown will get rendered in the dedicated
workflow "About" tab on your interface.

```python
@workflow
def foo(
    ...
):
    """This line is a short workflow description, displayed in the explore tab

    This line starts the long workflow description in markdown, displayed in
    this workflow's about tab

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
    ...
```

## Sidebar Customization

After the long workflow description, you have the opportunity to specify
standard workflow metadata that will get rendered in the sidebar. The currently
supported metadata fields are `display_name`, `documentation`, `author` (`name`, `email`,
`github`), `repository`, and `license`. The only constrained field is `license`, which
should be an identifier from [spdx](https://spdx.org/licenses/).

```yaml
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

When a workflow is registered, a each parameter will receive a frontend
component to ingest values in the browser. These components will perform
HTML-native type validation on inputted values and can be customized from
the python code.

The LatchBio Console currently supports customized parameter presentation in the
following ways:

* display names
* sections
* hidden parameters
* custom component presentations (eg. line vs paragraph presentation of `str`)

Developers can modify this custom presentation by writing statements in
[DSL](https://en.wikipedia.org/wiki/Domain-specific_language), either in YAML
format in the workflow function docstring or as JSON format specified in line
with Python's `typing.Annotated`.

Reading this documentation and browsing examples is the best way to become
familiar with this DSL.

### Display Name

This must be filled out for each parameter or the parameter will not render in
the interface. _This option must be specified in the docstring only._

```python
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
                display_name: Sample Size
        sample_name:
            __metadata__:
                display_name: Sample Name
    """
  ...
```

### Hidden Parameters

This option will move the parameter into the "hidden" section of the interface,
requiring a user to toggle a collapsed section to view and interact with it.

```python
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
  ...
```

### Section Titles

This adds a header before the specified parameter, indicating a semantic
grouping amongst the parameters that follow.

```python
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
                    section_title: Notification Options
    """
  ...
```

### Placeholders

An indication of sample input before user inputs anything.

Note that this is not a default value, as the user still needs to input a value
if one does not exist. The placeholder is strictly visual.

```python
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
  ...
```

### Comments

A clarification to be displayed next to the parameter name in parenthesis.

```python
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
  ...
```

### Output

Signals that a `LatchFile` or `LatchDir` is the output location of the workflow,
disabling path existence checks within the LatchBio console.

```python
@workflow
def test(
    sample_size: int,
    sample_name: str,
    sample_ph: Optional[float] = None,
    output_dir: LatchDir,
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
  ...
```

### Batch Table Column

Signals that a parameter should be displayed in the parameter preview when an
input row is collapsed in the batch view.

```python
@workflow
def test(
    sample_size: int,
    sample_name: str,
    sample_ph: Optional[float] = None,
    output_dir: LatchDir,
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
  ...
```

### Additional `str` Type Customizations

The default presentation of a `str` component is a "line". You can optionally
present the component as a "paragraph".

```python
@workflow
def test(
    sample_size: int,
    sample_name: str,
    sample_ph: Optional[float] = None,
    sample_description: Optional[str] = None,
    output_dir: LatchDir,
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
  ...
```

## Additional Rule-Based Validation

In addition to the HTML-native validation that each parameter component will
perform by default, developers can specify additional rule-based validation to
further restrict the set of accepted values. Rule-based validation is currently
specified with a [regex](https://docs.python.org/3/library/re.html#regular-expression-syntax) pattern.

Here is an example of how using rule-based validation can be used to construct a
"FastQ type" that only accepts a subset of semantic paths:

```python
from flytekit.core.annotation import FlyteAnnotation
from typing import Annotated

@workflow
def test(
    sample_list: List[
        Annotated[
            FlyteFile,
            FlyteAnnotation(
                {
                    "rules": [
                        {
                            "regex": "(.fasta|.fa|.faa)$",
                            "message": "Only .fasta, .fa, or .faa extensions are valid",
                        }
                    ]
                }
            ),
        ]
    ]
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
    ...
```
