# Customizing Your Interface

Latch dynamically constructs the workflow interface based on the workflow function code and the `LatchMetadata` object.

With these, you can specify

- the contact email, repository, social media links, etc.
- the ordering and grouping of parameters
- parameter tooltip descriptions
- parameter display names

## Parameter Display

The UI only displays parameters specified in the `LatchMetadata` object:

```python
from latch.types import LatchParameter, LatchAppearanceType, LatchRule

metadata = LatchMetadata(
    parameters: {
        'param_0': LatchParameter(
            display_name="Parameter 0",
            description="This is parameter 0",
            hidden=False,
        ),
        'param_1': LatchParameter(
            display_name="Parameter 1",
            description="This is parameter 1",
            hidden=True, # parameter is collapsed
        )
    }
)
...

@workflow(metadata)
def wf(
    param_0: int, # any supported type works
    param_1: str,
    ...
)
```

Each key in `metadata.parameters` must be the name of one of the parameters of the workflow function. Other keys will be ignored. Parameters without a corresponding key in `metadata.parameters` will not be displayed.

- {class}`~latch.types.metadata.LatchParameter` specifies the metadata associated with each workflow parameter.
- {class}`~latch.types.metadata.LatchAuthor` describes information about the workflow author.
- {class}`~latch.types.metadata.LatchRule` describes the rule that the parameter input must follow.

---

## Custom Parameter Layout (Flow)

By default, parameters are displayed in a flat list, in the order in which they are declared in the metadata. For more complex workflows it is often better to specify a custom layout, known as a "flow".

The custom layout is defined using the `flow` parameter of the {class}`~latch.types.metadata.Metadata` specification, which is a list of flow elements. Some flow elements can have child flows. All flow elements can be arbitrarily nested.

## Flow Elements

- {class}`~latch.types.metadata.Params` is the most basic flow element which displays the specified parameter widgets one after another. A parameter can be part of any number of {class}`~latch.types.metadata.Params` elements. The default workflow layout is equivalent to `flow=[Params("first_param", "second_param", ...)]`
- {class}`~latch.types.metadata.Title` and {class}`~latch.types.metadata.Text` are decorative flow elements that display a string of title and body text respectively. These elements have full markdown support. They are most often used to explain a parameter or group parameters without cluttering the UI as much as a {class}`~latch.types.metadata.Section`
- {class}`~latch.types.metadata.Section` displays a child flow in a card with a given title. This is the basic building block of most UIs
- {class}`~latch.types.metadata.Spoiler` displays a child flow in a collapsible card with a given title. The spoiler is collapsed by default. This is often used for hiding away parts of the UI that will not be useful to the majority of users
- {class}`~latch.types.metadata.Fork` shows a set of mutually-exclusive alternatives. The alternatives are specified as a list of {class}`~latch.types.metadata.ForkBranch`, each of which displays a child flow when active and nothing otherwise. Each branch is identified by a unique key. This key is passed to the workflow is a `str`-typed parameter so the user selection can be used to change runtime behavior

Visit the API docs for an example of how to use each flow element.
