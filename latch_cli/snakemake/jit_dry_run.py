from pathlib import Path

import click

from .serialize import generate_jit_register_code
from .utils import load_snakemake_metadata
from .workflow import build_jit_register_wrapper


@click.command()
@click.argument(
    "pkg_root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option(
    "--snakefile",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to a Snakefile.",
)
@click.option(
    "--test-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to a test file specifying workflow inputs.",
)
def snakemake_dry_run(pkg_root: Path, snakefile: Path, test_file: Path):
    meta_file = load_snakemake_metadata(pkg_root)
    if meta_file is not None:
        click.echo(f"Using metadata file {click.style(meta_file, italic=True)}")
    else:
        raise ValueError(
            f"Could not find metadata file in {click.style(pkg_root, italic=True)}"
        )

    click.echo(f"Re-generating JIT register code with dry_run flag")
    jit_wf = build_jit_register_wrapper(False)
    generate_jit_register_code(
        jit_wf,
        pkg_root,
        snakefile,
        dry_run=True,
    )

    click.echo(
        f"Executing workflow with test file {click.style(test_file, italic=True)}"
    )
    exec(open(test_file).read())


if __name__ == "__main__":
    snakemake_dry_run()
