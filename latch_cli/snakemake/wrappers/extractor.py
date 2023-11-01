from pathlib import Path
from typing import List, Set

from snakemake.workflow import Workflow


class SnakemakeWrapperExtractor(Workflow):
    def __init__(self, pkg_root: Path, snakefile: Path):
        self.pkg_root = pkg_root

        super().__init__(snakefile=snakefile)

    def extract_wrappers(self) -> Set[str]:
        wrappers: Set[str] = set()

        for rule in self.rules:
            if not rule.is_wrapper:
                continue

            wrappers.add(rule.wrapper)

        return wrappers
