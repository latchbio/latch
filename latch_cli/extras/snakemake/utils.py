import re
from pathlib import Path
from typing import Dict, Optional, Union

from latch_cli.utils import urljoins

from ...services.register.utils import import_module_by_path


def update_mapping(cur: Path, stem: Path, remote: str, mapping: Dict[str, str]):
    if cur.is_file():
        mapping[str(stem)] = remote
        return

    for p in cur.iterdir():
        update_mapping(p, stem / p.name, urljoins(remote, p.name), mapping)


underscores = re.compile(r"_+")


def best_effort_display_name(x: str) -> str:
    return underscores.sub(" ", x).title().strip()


def load_snakemake_metadata(pkg_root: Path) -> Optional[Path]:
    new_meta = pkg_root / "latch_metadata" / "__init__.py"
    old_meta = pkg_root / "latch_metadata.py"

    if new_meta.exists():
        import_module_by_path(new_meta)

        return new_meta
    elif old_meta.exists():
        import_module_by_path(old_meta)

        return old_meta
