from pathlib import Path
from typing import List

from snakemake.rules import Rule
from snakemake.workflow import Workflow


class SnakemakeWrapperExtractor(Workflow):
    def __init__(self, pkg_root: Path, snakefile: Path):
        self.pkg_root = pkg_root

        super().__init__(snakefile=snakefile)

    def extract_wrappers(self) -> List[str]:
        wrappers: List[str] = []

        rule: Rule
        for rule in self.rules:
            if not rule.is_wrapper:
                continue

            wrappers.append(rule.wrapper)

        return wrappers
