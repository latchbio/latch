from pathlib import Path
from typing import Optional

from ..services.register.utils import import_module_by_path


def load_snakemake_metadata(pkg_root: Path) -> Optional[Path]:
    new_meta = pkg_root / "latch_metadata" / "__init__.py"
    old_meta = pkg_root / "latch_metadata.py"

    if new_meta.exists():
        import_module_by_path(new_meta)

        return new_meta
    elif old_meta.exists():
        import_module_by_path(old_meta)

        return old_meta
