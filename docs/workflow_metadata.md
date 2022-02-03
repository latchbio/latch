# Describing Your Workflow

## Short and Long Workflow Description

Adding a description to your workflow is easily achieved by following the workflow metadata format. You provide two descriptions, one short one displayed in the workflow list (in explore or in workflows) and a longer description which is rendered in markdown on the workflow about page.

```
@workflow
def test(
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

## Standardized Workflow Metadata

After the long workflow description, you have the opportunity to specify standard workflow metadata. The currently supported metadata fields are display name, documentation, author, repository, and license. The only constrained field is license, which should be an identifier from [spdx](https://spdx.org/licenses/).

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