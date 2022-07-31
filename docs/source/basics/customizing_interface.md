# Customizing Your Interface

The Latch SDK will dynamically construct parameter interfaces from your python code. With these you can specify

* markdown formatted long form documentation
* sidebar presentation of contact email, repository, social media links, etc.
* the ordering and grouping of parameters
* parameter tooltip descriptions
* parameter display names

You use the `LatchMetadata`, `LatchParameter`, etc. constructs to create your parameter interface, with a docstring specifying a short and long description.

---

## Using `LatchMetdata` objects

While most of the metadata of a workflow will be encapsulated in a `LatchMetadata` object, we still require a docstring in the body of the workflow function which specifies both a short and long-form description.

### One Line Description

The first line of the workflow function docstring will get rendered in the sidebar of the workflow and the workflow explore tab as a brief description of your workflow's functionality. Think of this as summarizing the entirety of your workflow's significance into a single line.

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

### Long Form Description

The body of the workflow function docstring is where you write long-form markdown documentation. This markdown will get rendered in the dedicated workflow "About" tab on your interface. Feel free to include links, lists, code blocks, and more.

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

    ### headers

    Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod 
    tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, 
    quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo 
    consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse 
    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat 
    non proident, sunt in culpa qui officia deserunt mollit anim id est 
    laborum.
    """
    ...
```

### The `LatchMetadata` Object

The main object that organizes the metadata for a workflow is the `LatchMetadata` object. To use, create a singleton instance of a `LatchMetadata` object as follows:

```python
from latch.types import LatchMetadata, LatchAuthor

metadata = LatchMetadata(
    display_name="My Workflow",
    documentation="https://github.com/author/my_workflow/README.md",
    author=LatchAuthor(
        name="Workflow Author",
        email="licensing@company.com",
        github="https://github.com/author",
    ),
    repository="https://github.com/author/my_workflow",
    license="MIT",
)
```

The information given here will be rendered in the sidebar of the workflow in the Latch Console. Here's a brief description of each of the fields of the LatchMetadata object:

* `display_name`: The name of the workflow, e.g. CRISPResso2,
* `documentation`: A URL that leads to documentation for the workflow itself,
* `author`: This must be a `LatchAuthor` objects, whose fields are:
  * `name`: The name of the author;
  * `email`: The author's email;
  * `github`: A link to the author's Github profile,
* `repository`: A link to the Github repository where the code for the workflow lives,
* `license`: The license that the workflow code falls under - must be a [SPDX](https://spdx.dev/) identifier.

### Customizing Parameter Presentation

When a workflow is registered, each parameter will receive a frontend component to ingest values in the browser. These components will perform HTML-native type validation on inputted values and can be customized from the python code. You can add a parameter to the interface by adding a `LatchParameter` object to your `LatchMetadata` object's parameter dictionary as below:

```python
from latch.types import LatchParameter, LatchAppearanceType, LatchRule

...

# Assuming you have created a LatchMetadata object named `metadata`
metadata.parameters['param_0'] = LatchParameter(
    display_name="Parameter 0",
    description="This is parameter 0",
    hidden=False,
)

...

@workflow(metadata)
def wf(
    param_0: int, # any of the supported types would also work here
    ...
)
```

Each key in `metadata.parameters` must be the name of one of the parameters of the workflow, and so the corresponding `LatchParameter` object describes that specific parameter. A `LatchParameter` can take a myriad of keyword arguments at construction time, each of which are briefly described below.

* `display_name`: A human-readable, descriptive name of the parameter,
* `description`: A short description of the role of the parameter within the workflow, to be displayed when hovered over in a tooltip,
* `hidden`: A boolean for whether or not the parameter should be hidden by default,
* `section_title`: If provided, the specified parameter will start a new section of the given name,
* `placeholder`: What placeholder to put inside the input form for the parameter if no value is present,
* `comment`: A comment about the parameter,
* `output`: Whether this parameter is an output directory (to disable path existence checks),
* `batch_table_column`: Whether this parameter should have a column to itself in the batch table at the top of the parameters page,
* `appearance_type`: Either `LatchAppearanceType.line` or `LatchAppearanceType.paragraph`, which style to render text inputs as.
* `rules`: A list of `LatchRule`s which consist of a regular expression and a message. If provided, an input must match all given regexes in order to appear valid in the front end - if it fails to match one of the regexes, the corresponding message is displayed.
