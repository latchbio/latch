"""
Complete integration example showing how to use the flexible type converter.

This example demonstrates launching a workflow with complex types without
requiring exact imports from the workflow module.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

# Imagine this is your workflow definition (already registered on Latch):
"""
# workflow.py (on Latch)

from dataclasses import dataclass
from enum import Enum
from latch import workflow
from latch.types.file import LatchFile

@dataclass
class SampleInfo:
    sample_id: str
    replicate: int
    condition: str
    metadata: dict

class QualityThreshold(Enum):
    LOW = 20
    MEDIUM = 30
    HIGH = 40

class ProcessingMode(Enum):
    FAST = "fast"
    BALANCED = "balanced"
    THOROUGH = "thorough"

@workflow
def genomics_pipeline(
    samples: List[SampleInfo],
    reference: LatchFile,
    quality_threshold: QualityThreshold,
    mode: ProcessingMode,
    output_prefix: str,
    max_threads: int = 8,
    enable_qc: bool = True,
    email_notify: Optional[str] = None,
):
    # Workflow implementation...
    pass
"""


# ============================================================================
# LAUNCH EXAMPLES - No imports from workflow.py needed!
# ============================================================================


def example_1_basic_launch():
    """Basic launch with all required parameters."""
    from latch_cli.services.launch.launch_v2 import launch

    execution = launch(
        wf_name="genomics_pipeline",
        params={
            # List of dicts instead of SampleInfo dataclass instances
            "samples": [
                {
                    "sample_id": "SAMPLE001",
                    "replicate": 1,
                    "condition": "control",
                    "metadata": {"batch": "A", "date": "2024-01-15"},
                },
                {
                    "sample_id": "SAMPLE002",
                    "replicate": 2,
                    "condition": "treatment",
                    "metadata": {"batch": "A", "date": "2024-01-15"},
                },
            ],
            # File path as string (or LatchFile if you want)
            "reference": "latch:///reference/hg38.fa",
            # Integer value instead of QualityThreshold enum
            "quality_threshold": 30,  # MEDIUM quality
            # String value instead of ProcessingMode enum
            "mode": "balanced",  # BALANCED mode
            # Simple types work as before
            "output_prefix": "experiment_001",
            "max_threads": 16,
            "enable_qc": True,
            # None for optional parameter
            "email_notify": None,
        },
    )

    print(f"Launched execution: {execution.id}")
    return execution


def example_2_with_dataclasses():
    """Launch using local dataclasses (not imported from workflow)."""
    from latch_cli.services.launch.launch_v2 import launch

    # Define local versions of the workflow types
    @dataclass
    class MySampleInfo:
        sample_id: str
        replicate: int
        condition: str
        metadata: dict

    # Create sample objects
    samples = [
        MySampleInfo(
            sample_id="SAMPLE003",
            replicate=1,
            condition="drug_a",
            metadata={"concentration": "10uM", "duration": "24h"},
        ),
        MySampleInfo(
            sample_id="SAMPLE004",
            replicate=2,
            condition="drug_a",
            metadata={"concentration": "10uM", "duration": "24h"},
        ),
    ]

    execution = launch(
        wf_name="genomics_pipeline",
        params={
            "samples": samples,  # Local dataclass instances work!
            "reference": "latch:///reference/mm10.fa",
            "quality_threshold": 40,  # HIGH
            "mode": "thorough",
            "output_prefix": "drug_study",
            "email_notify": "researcher@example.com",
        },
    )

    print(f"Launched execution: {execution.id}")
    return execution


def example_3_with_local_enums():
    """Launch using local enum definitions."""
    from latch_cli.services.launch.launch_v2 import launch

    # Define local enums (same values as workflow enums)
    class MyQualityThreshold(Enum):
        LOW = 20
        MEDIUM = 30
        HIGH = 40

    class MyProcessingMode(Enum):
        FAST = "fast"
        BALANCED = "balanced"
        THOROUGH = "thorough"

    execution = launch(
        wf_name="genomics_pipeline",
        params={
            "samples": [
                {"sample_id": "S001", "replicate": 1, "condition": "A", "metadata": {}}
            ],
            "reference": "latch:///ref.fa",
            # Use local enum instances
            "quality_threshold": MyQualityThreshold.HIGH,
            "mode": MyProcessingMode.FAST,
            "output_prefix": "test_run",
        },
    )

    print(f"Launched execution: {execution.id}")
    return execution


def example_4_mixed_approach():
    """Mix different approaches for maximum flexibility."""
    from latch_cli.services.launch.launch_v2 import launch

    # Some samples as dicts, some as objects
    class SimpleSample:
        def __init__(self, sample_id, replicate, condition):
            self.sample_id = sample_id
            self.replicate = replicate
            self.condition = condition
            self.metadata = {"source": "simple_object"}

    execution = launch(
        wf_name="genomics_pipeline",
        params={
            "samples": [
                # Mix different types - all work!
                {"sample_id": "S1", "replicate": 1, "condition": "C1", "metadata": {}},
                SimpleSample("S2", 2, "C2"),
                {"sample_id": "S3", "replicate": 3, "condition": "C3", "metadata": {}},
            ],
            "reference": "latch:///reference/genome.fa",
            "quality_threshold": 30,
            "mode": "balanced",
            "output_prefix": "mixed_test",
        },
    )

    print(f"Launched execution: {execution.id}")
    return execution


async def example_5_launch_and_wait():
    """Launch a workflow and wait for completion."""
    from latch_cli.services.launch.launch_v2 import launch

    execution = launch(
        wf_name="genomics_pipeline",
        params={
            "samples": [
                {"sample_id": "TEST", "replicate": 1, "condition": "test", "metadata": {}}
            ],
            "reference": "latch:///reference/test.fa",
            "quality_threshold": 20,
            "mode": "fast",
            "output_prefix": "quick_test",
            "max_threads": 4,
        },
    )

    print(f"Launched execution: {execution.id}")
    print("Waiting for completion...")

    # Wait for the execution to complete
    result = await execution.wait()

    if result is None:
        print("Execution did not complete properly")
        return

    if result.status == "SUCCEEDED":
        print(f"Execution succeeded!")
        print(f"Outputs: {result.output}")
        print(f"Ingress data: {result.ingress_data}")
    elif result.status == "FAILED":
        print(f"Execution failed")
    elif result.status == "ABORTED":
        print(f"Execution was aborted")

    return result


def example_6_error_handling():
    """Demonstrate error handling with clear messages."""
    from latch_cli.services.launch.launch_v2 import launch

    try:
        execution = launch(
            wf_name="genomics_pipeline",
            params={
                # This will fail - samples should be a list, not a dict
                "samples": {"this": "is wrong"},
                "reference": "latch:///ref.fa",
                "quality_threshold": 30,
                "mode": "balanced",
                "output_prefix": "error_test",
            },
        )
    except ValueError as e:
        # Will get a clear error message:
        # "Failed to convert parameter 'samples' with value {...}: Expected list or tuple, got <class 'dict'>"
        print(f"Error: {e}")
        return None


# ============================================================================
# COMPARISON: Before vs After
# ============================================================================


def before_flexible_types():
    """How you HAD to do it before (exact imports required)."""
    # This would fail if imports don't match exactly!

    # from some.workflow.module import SampleInfo, QualityThreshold, ProcessingMode
    # from latch_cli.services.launch.launch_v2 import launch
    #
    # execution = launch(
    #     wf_name="genomics_pipeline",
    #     params={
    #         "samples": [
    #             SampleInfo(  # Must be the EXACT class from workflow
    #                 sample_id="S001",
    #                 replicate=1,
    #                 condition="control",
    #                 metadata={}
    #             )
    #         ],
    #         "quality_threshold": QualityThreshold.MEDIUM,  # Must be EXACT enum
    #         "mode": ProcessingMode.BALANCED,  # Must be EXACT enum
    #         ...
    #     }
    # )

    pass


def after_flexible_types():
    """How you can do it now (no imports needed)."""
    from latch_cli.services.launch.launch_v2 import launch

    # No imports from workflow module needed!
    execution = launch(
        wf_name="genomics_pipeline",
        params={
            "samples": [
                {  # Just a dict!
                    "sample_id": "S001",
                    "replicate": 1,
                    "condition": "control",
                    "metadata": {},
                }
            ],
            "reference": "latch:///ref.fa",
            "quality_threshold": 30,  # Just the value!
            "mode": "balanced",  # Just the string!
            "output_prefix": "test",
        },
    )

    return execution


if __name__ == "__main__":
    print("=" * 70)
    print("Type Converter Integration Examples")
    print("=" * 70)

    # Run examples
    print("\n[1] Basic launch with dicts and raw values...")
    # example_1_basic_launch()

    print("\n[2] Launch with local dataclasses...")
    # example_2_with_dataclasses()

    print("\n[3] Launch with local enums...")
    # example_3_with_local_enums()

    print("\n[4] Mixed approach...")
    # example_4_mixed_approach()

    print("\n[5] Launch and wait (async)...")
    # import asyncio
    # asyncio.run(example_5_launch_and_wait())

    print("\n[6] Error handling...")
    # example_6_error_handling()

    print("\n" + "=" * 70)
    print("Examples complete! Uncomment the function calls to run them.")
    print("=" * 70)
