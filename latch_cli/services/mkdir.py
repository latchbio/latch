import click

from latch.ldata.path import LatchPathError, LPath


def mkdirp(remote_directory):
    """Creates an empty directory on Latch

    Args:
        remote_directory:   A valid path to a remote destination, of the form

                                [latch://] [/] dir_1/dir_2/.../dir_n/dir_name,

                            where dir_name is the name of the new directory to be created.
                            Every directory in the path (dir_i) must already exist.

    This function will create a directory at the specified path in Latch. Will error if
    the path is invalid or if an upstream directory does not exist. If a directory with the
    same name already exists, this will make a new directory with an indexed name (see below).

    Example: ::

        mkdirp("sample") # sample doesn't already exist

            Creates a new empty directory visible in Latch Console called sample, located in
            the root of the user's Latch filesystem

        mkdirp("/dir1/doesnt_exist/dir2/") # doesnt_exist doesn't exist

            Creates a two new directories visible in Latch Console called "doesnt_exist" and
            "dir2", located in the directory dir1.
    """
    try:
        LPath(remote_directory).mkdirp()
    except LatchPathError as e:
        click.secho(str(e), fg="red")
        raise click.exceptions.Exit(1) from e
    click.secho(f"Successfully created {remote_directory}.", fg="green")
