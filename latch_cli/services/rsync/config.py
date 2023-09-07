from dataclasses import dataclass


@dataclass(frozen=True)
class RsyncConfig:
    verbose: bool
