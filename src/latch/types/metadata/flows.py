from dataclasses import dataclass


@dataclass(frozen=True)
class FlowBase:
    """Parent class for all flow elements

    Available flow elements:

    * :class:`~latch.types.metadata.Params`

    * :class:`~latch.types.metadata.Text`

    * :class:`~latch.types.metadata.Title`

    * :class:`~latch.types.metadata.Section`

    * :class:`~latch.types.metadata.Spoiler`

    * :class:`~latch.types.metadata.Fork`
    """


@dataclass(frozen=True, init=False)
class Section(FlowBase):
    """Flow element that displays a child flow in a card with a given title

    Example:

    .. image:: ../assets/flow-example/flow_example_1.png
        :alt: Example of a user interface for a workflow with a custom flow

    .. image:: ../assets/flow-example/flow_example_spoiler.png
        :alt: Example of a spoiler flow element


    The `LatchMetadata` for the example above can be defined as follows:

    .. code-block:: python

        from latch.types import LatchMetadata, LatchParameter
        from latch.types.metadata import FlowBase, Section, Text, Params, Fork, Spoiler
        from latch import workflow

        flow = [
            Section(
                "Samples",
                Text(
                    "Sample provided has to include an identifier for the sample (Sample name)"
                    " and one or two files corresponding to the reads (single-end or paired-end, respectively)"
                ),
                Fork(
                    "sample_fork",
                    "Choose read type",
                    paired_end=ForkBranch("Paired-end", Params("paired_end")),
                    single_end=ForkBranch("Single-end", Params("single_end")),
                ),
            ),
            Section(
                "Quality threshold",
                Text(
                    "Select the quality value in which a base is qualified."
                    "Quality value refers to a Phred quality score"
                ),
                Params("quality_threshold"),
            ),
            Spoiler(
                "Output directory",
                Text("Name of the output directory to send results to."),
                Params("output_directory"),
            ),
        ]

        metadata = LatchMetadata(
            display_name="fastp - Flow Tutorial",
            author=LatchAuthor(
                name="LatchBio",
            ),
            parameters={
                "sample_fork": LatchParameter(),
                "paired_end": LatchParameter(
                    display_name="Paired-end reads",
                    description="FASTQ files",
                    batch_table_column=True,
                ),
                "single_end": LatchParameter(
                    display_name="Single-end reads",
                    description="FASTQ files",
                    batch_table_column=True,
                ),
                "output_directory": LatchParameter(
                    display_name="Output directory",
                ),
            },
            flow=flow,
        )

        @workflow(metadata)
        def fastp(
            sample_fork: str,
            paired_end: PairedEnd,
            single_end: Optional[SingleEnd] = None,
            output_directory: str = "fastp_results",
        ) -> LatchDir:
            ...
    """

    section: str
    """Title of the section"""
    flow: list[FlowBase]
    """Flow displayed in the section card"""

    def __init__(self, section: str, *flow: FlowBase):
        object.__setattr__(self, "section", section)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True)
class Text(FlowBase):
    """Flow element that displays a markdown string"""

    text: str
    """Markdown body text"""


@dataclass(frozen=True)
class Title(FlowBase):
    """Flow element that displays a markdown title"""

    title: str
    """Markdown title text"""


@dataclass(frozen=True, init=False)
class Params(FlowBase):
    """Flow element that displays parameter widgets"""

    params: list[str]
    """
    Names of parameters whose widgets will be displayed.
    Order is preserved. Duplicates are allowed
    """

    def __init__(self, *args: str):
        object.__setattr__(self, "params", list(args))


@dataclass(frozen=True, init=False)
class Spoiler(FlowBase):
    """Flow element that displays a collapsible card with a given title"""

    spoiler: str
    """Title of the spoiler"""
    flow: list[FlowBase]
    """Flow displayed in the spoiler card"""

    def __init__(self, spoiler: str, *flow: FlowBase):
        object.__setattr__(self, "spoiler", spoiler)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class ForkBranch:
    """Definition of a :class:`~latch.types.metadata.Fork` branch"""

    display_name: str
    """String displayed in the fork's multibutton"""
    flow: list[FlowBase]
    """Child flow displayed in the fork card when the branch is active"""

    def __init__(self, display_name: str, *flow: FlowBase):
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flow", list(flow))


@dataclass(frozen=True, init=False)
class Fork(FlowBase):
    """Flow element that displays a set of mutually exclusive alternatives

    Displays a title, followed by a horizontal multibutton for selecting a branch,
    then a card for the active branch
    """

    fork: str
    """Name of a `str`-typed parameter to store the active branch's key"""
    display_name: str
    """Title shown above the fork selector"""
    flows: dict[str, ForkBranch]
    """
    Mapping between branch keys to branch definitions.
    Order determines the order of options in the multibutton
    """

    def __init__(self, fork: str, display_name: str, **flows: ForkBranch):
        object.__setattr__(self, "fork", fork)
        object.__setattr__(self, "display_name", display_name)
        object.__setattr__(self, "flows", flows)
