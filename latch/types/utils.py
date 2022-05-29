from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

from latch.types import LatchFile


def file_glob(
    pattern: str, remote_directory: str, target_dir: Optional[Path] = None
) -> List[LatchFile]:
    """Constructs a list of LatchFiles from a glob pattern.

    Convenient utility for passing collections of files between tasks. See
    [nextflow's channels](https://www.nextflow.io/docs/latest/channel.html) or
    [snakemake's wildcards](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#wildcards).
    for similar functionality in other orchestration tools.

    The remote location of each constructed LatchFile will be consructed by
    appending the file name returned by the pattern to the directory
    represented by the `remote_directory`.

    Args:
        pattern: A glob pattern to match a set of files, eg. '*.py'. Will
            resolve paths with respect to the working directory of the caller.
        remote_directory: A valid latch URL pointing to a directory, eg.
            latch:///foo. This _must_ be a directory and not a file.
        target_dir: An optional Path object to define an alternate working
            directory for path resolution

    Returns:
        A list of instantiated LatchFile objects.

    Intended Use: ::

        @small_task
        def task():

            ...

            return file_glob("*.fastq.gz", "latch:///fastqc_outputs")

    """

    _validate_latch_url(remote_directory)

    if target_dir is None:
        wd = Path(".")
    else:
        wd = target_dir
    matched = sorted(wd.glob(pattern))

    return [LatchFile(file, remote_directory + file.name) for file in matched]


class InvalidLatchURL(Exception):
    pass


def _validate_latch_url(url: str):

    try:
        parsed = urlparse(url)
    except ValueError as e:
        raise InvalidLatchURL(
            f"{url} is not a valid url. See"
            " https://docs.latch.bio/basics/working_with_files.html#latch-urls."
        ) from e

    if parsed.scheme != "latch":
        raise InvalidLatchURL(
            f"{url} is not a valid latch url - must use the 'latch' scheme. See"
            " https://docs.latch.bio/basics/working_with_files.html#latch-urls."
        )

    if not parsed.path.startswith("/"):
        raise InvalidLatchURL(
            f"{url} is not a valid latch url - does not contain an absolute"
            " path within the url. (It is common to forget the third backslash in"
            " a the beginning of the string, eg.`latch: ///foobar.txt`, a"
            " correctly formatted latch URL string.)"
        )
