from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import click

from latch.ldata._transfer.download import download as _download
from latch.ldata._transfer.progress import Progress
from latch.ldata._transfer.remote_copy import remote_copy as _remote_copy
from latch.ldata._transfer.upload import upload as _upload
from latch.ldata.type import LatchPathError
from latch_cli.services.cp.glob import expand_pattern
from latch_cli.utils import human_readable_time, with_si_suffix
from latch_cli.utils.path import get_path_error, is_remote_path


def _copy_and_print(src: str, dst: str, progress: Progress) -> None:
    _remote_copy(src, dst)
    if progress != Progress.none:
        click.echo(dedent(f"""
            {click.style("Copy Requested.", fg="green")}
            {click.style("Source: ", fg="blue")}{(src)}
            {click.style("Destination: ", fg="blue")}{(dst)}"""))


def _download_and_print(src: str, dst: Path, progress: Progress, verbose: bool) -> None:
    if progress != Progress.none:
        click.secho(f"Downloading {dst.name}", fg="blue")
    res = _download(src, dst, progress, verbose)
    if progress != Progress.none:
        click.echo(dedent(f"""
			{click.style("Download Complete", fg="green")}
			{click.style("Time Elapsed: ", fg="blue")}{human_readable_time(res.total_time)}
			{click.style("Files Downloaded: ", fg="blue")}{res.num_files} ({with_si_suffix(res.total_bytes)})
			"""))


# todo(ayush): come up with a better behavior scheme than unix cp
def cp(
    srcs: List[str],
    dest: str,
    *,
    progress: Progress,
    verbose: bool,
    expand_globs: bool,
    cores: Optional[int] = None,
    chunk_size_mib: Optional[int] = None,
):
    if chunk_size_mib is not None and chunk_size_mib < 5:
        click.secho(
            "The chunk size specified by --chunk-size-mib must be at least 5. You"
            f" provided `{chunk_size_mib}`",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    dest_remote = is_remote_path(dest)

    for src in srcs:
        src_remote = is_remote_path(src)

        try:
            if src_remote and not dest_remote:
                if expand_globs:
                    [
                        _download_and_print(p, Path(dest), progress, verbose)
                        for p in expand_pattern(src)
                    ]
                else:
                    _download_and_print(src, Path(dest), progress, verbose)
            elif not src_remote and dest_remote:
                if progress != Progress.none:
                    click.secho(f"Uploading {src}", fg="blue")
                res = _upload(
                    src,
                    dest,
                    progress=progress,
                    verbose=verbose,
                    cores=cores,
                    chunk_size_mib=chunk_size_mib,
                )
                if progress != Progress.none:
                    click.echo(dedent(f"""
                        {click.style("Upload Complete", fg="green")}
                        {click.style("Time Elapsed: ", fg="blue")}{human_readable_time(res.total_time)}
                        {click.style("Files Uploaded: ", fg="blue")}{res.num_files} ({with_si_suffix(res.total_bytes)})
                        """))
            elif src_remote and dest_remote:
                if expand_globs:
                    [_copy_and_print(p, dest, progress) for p in expand_pattern(src)]
                else:
                    _copy_and_print(src, dest, progress)
            else:
                raise ValueError(
                    dedent(f"""
                    `latch cp` cannot be used for purely local file copying.

                    Please ensure at least one of your arguments is a remote path (beginning with `latch://`)
                    """).strip("\n"),
                )
        except LatchPathError as e:
            click.secho(get_path_error(e.remote_path, e.message, e.acc_id), fg="red")
            raise click.exceptions.Exit(1) from e
        except Exception as e:
            click.secho(str(e), fg="red")
            raise click.exceptions.Exit(1) from e
