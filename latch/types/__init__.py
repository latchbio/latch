from latch._deprecation import _deprecated_import
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile, LatchOutputFile
from latch.types.glob import file_glob
from latch.types.metadata import (
    DockerMetadata,
    Fork,
    ForkBranch,
    LatchAppearanceType,
    LatchAuthor,
    LatchMetadata,
    LatchParameter,
    LatchRule,
    Params,
    Section,
    Spoiler,
    Text,
)

# LatchDir = _deprecated_import("LatchDir", "latch.types.directory")(LatchDir)
# LatchOutputDir = _deprecated_import("LatchOutputDir", "latch.types.directory")(
#     LatchOutputDir
# )

# LatchFile = _deprecated_import("LatchFile", "latch.types.file")(LatchFile)
# LatchOutputFile = _deprecated_import("LatchOutputFile", "latch.types.file")(
#     LatchOutputFile
# )

# file_glob = _deprecated_import("file_glob", "latch.types.glob")(file_glob)

# Fork = _deprecated_import("Fork", "latch.types.metadata")(Fork)
# ForkBranch = _deprecated_import("ForkBranch", "latch.types.metadata")(ForkBranch)
# LatchAppearanceType = _deprecated_import("LatchAppearanceType", "latch.types.metadata")(
#     LatchAppearanceType
# )
# LatchAuthor = _deprecated_import("LatchAuthor", "latch.types.metadata")(LatchAuthor)
# LatchMetadata = _deprecated_import("LatchMetadata", "latch.types.metadata")(
#     LatchMetadata
# )
# LatchParameter = _deprecated_import("LatchParameter", "latch.types.metadata")(
#     LatchParameter
# )
# LatchRule = _deprecated_import("LatchRule", "latch.types.metadata")(LatchRule)
# Params = _deprecated_import("Params", "latch.types.metadata")(Params)
# Section = _deprecated_import("Section", "latch.types.metadata")(Section)
# Spoiler = _deprecated_import("Spoiler", "latch.types.metadata")(Spoiler)
# Text = _deprecated_import("Text", "latch.types.metadata")(Text)
