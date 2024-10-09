from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import click
import gql
from latch_sdk_gql.execute import execute

from latch.ldata._transfer.download import download as _download
from latch.ldata._transfer.progress import Progress
from latch.ldata._transfer.remote_copy import remote_copy as _remote_copy
from latch.ldata._transfer.upload import upload as _upload
from latch.ldata.type import LatchPathError
from latch_cli.services.cp.glob import expand_pattern
from latch_cli.utils import human_readable_time, with_si_suffix
from latch_cli.utils.path import get_path_error, is_remote_path

from .download.main import download
from .upload.main import upload


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

    # todo(ayush): make this a global thats computed in requires_auth()
    acc_info = execute(gql.gql("""
        query AccountInfo {
            accountInfoCurrent {
                id
            }
        }
    """))["accountInfoCurrent"]

    acc_id = acc_info["id"]

    dest_remote = is_remote_path(dest)
    srcs_remote = [is_remote_path(src) for src in srcs]

    try:
        if not dest_remote and all(srcs_remote):
            download(
                srcs,
                Path(dest),
                progress.name,
                verbose,
                expand_globs,
                cores,
                chunk_size_mib,
            )
        elif dest_remote and not any(srcs_remote):
            upload(
                srcs,
                dest,
                progress.name,
                verbose,
                cores,
                chunk_size_mib,
            )
        elif dest_remote and all(srcs_remote):
            for src in srcs:
                if expand_globs:
                    [_copy_and_print(p, dest, progress) for p in expand_pattern(src)]
                else:
                    _copy_and_print(src, dest, progress)
        else:
            click.secho(
                dedent(f"""\
                    Invalid arguments. The following argument types are valid:

                    (1) All source arguments are remote paths and destination argument is local (download)
                    (2) All source arguments are local paths and destination argument is remote (upload)
                    (3) All source arguments are remote paths and destination argument is remote (remote copy)\
                """),
                fg="red",
            )
            raise click.exceptions.Exit(1)

    except LatchPathError as e:
        click.secho(get_path_error(e.remote_path, e.message, acc_id), fg="red")
        raise click.exceptions.Exit(1) from e
    except Exception as e:
        click.secho(str(e), fg="red")
        raise click.exceptions.Exit(1) from e
