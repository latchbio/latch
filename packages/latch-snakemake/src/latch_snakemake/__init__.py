from pathlib import Path
from typing import Optional

import click

from latch_cli.main import main
from latch_cli.services.register.utils import import_module_by_path

from .workflow import get_entrypoint_content


@main.group()
def snakemake():
    """Manage snakemake-specific commands"""


@snakemake.command("generate-entrypoint")
@click.argument("pkg-root", nargs=1, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--metadata-root",
    type=click.Path(exists=True, path_type=Path, file_okay=False),
    help="Path to a directory containing a python package defining a SnakemakeV2Metadata "
    "object. If not provided, will default to searching the package root for a directory called "
    "`latch_metadata`.",
)
@click.option(
    "--snakefile",
    required=False,
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
    help="Path to the Snakefile to register. If not provided, will default to searching the package "
    "root for a file named `Snakefile`.",
)
def sm_generate_entrypoint(
    pkg_root: Path, metadata_root: Optional[Path], snakefile: Optional[Path]
):
    """Generate a `wf/entrypoint.py` file from a Snakemake workflow"""

    dest = pkg_root / "wf" / "entrypoint.py"
    dest.parent.mkdir(exist_ok=True)

    if dest.exists() and not click.confirm(
        f"Workflow entrypoint already exists at `{dest}`. Overwrite?"
    ):
        return

    if metadata_root is None:
        metadata_root = pkg_root / "latch_metadata"

    metadata_path = metadata_root / "__init__.py"
    if metadata_path.exists():
        click.echo(f"Using metadata file {click.style(metadata_path, italic=True)}")
        import_module_by_path(metadata_path)
    else:
        click.secho(
            f"Unable to find file `{metadata_path}` with a `SnakemakeV2Metadata` object "
            "defined. If you have a custom metadata root please provide a path "
            "to it using the `--metadata-root` option",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    import latch.types.metadata.snakemake_v2 as metadata

    if metadata._snakemake_v2_metadata is None:
        click.secho(
            "Failed to generate entrypoint. Make sure the python package at path "
            f"`{metadata_path}` defines a `SnakemakeV2Metadata` object.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if snakefile is None:
        snakefile = pkg_root / "Snakefile"

    if not snakefile.exists():
        click.secho(
            f"Unable to find a Snakefile at `{snakefile}`. If your Snakefile is "
            "in a different location please provide an explicit path to it "
            "using the `--snakefile` option."
        )
        raise click.exceptions.Exit(1)

    dest.write_text(get_entrypoint_content(pkg_root, metadata_path, snakefile))
