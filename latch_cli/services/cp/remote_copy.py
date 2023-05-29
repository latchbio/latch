import click
import gql

from latch.gql._execute import execute
from latch_cli.services.cp.ldata_utils import get_node_data
from latch_cli.services.cp.path_utils import get_path_error


# todo(ayush): figure out how to do progress for this
def remote_copy(
    src: str,
    dest: str,
):
    acc_id, data = get_node_data(src, dest, allow_resolve_to_parent=True)

    if data[src].is_parent:
        raise get_path_error(dest, "not found", acc_id)
    if not data[dest].is_parent:
        raise get_path_error(
            dest, "node already exists at path, refusing to copy", acc_id
        )

    execute(
        gql.gql("""
        mutation Copy($argSrcNode: BigInt!, $argDstParent: BigInt!) {
            ldataCopy(input: {
                argSrcNode: $argSrcNode,
                argDstParent: $argDstParent
            }) {
                clientMutationId
            }
        }"""),
        {"argSrcNode": data[src].id, "argDstParent": data[dest].id},
    )

    click.echo(f"""
{click.style("Copy Requested", fg="green")}

{click.style("Source: ", fg="blue")}{(src)}
{click.style("Destination: ", fg="blue")}{(dest)}""")
