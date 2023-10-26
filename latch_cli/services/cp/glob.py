import re
from typing import List

from latch_cli.services.cp.ldata_utils import _get_immediate_children_of_node
from latch_cli.utils import urljoins


def expand_pattern(remote_path: str) -> List[str]:
    parent, pattern = tuple(remote_path.rsplit("/", 1))

    if "*" not in pattern:
        return [remote_path]

    pattern_re = re.compile(re.escape(pattern).replace(r"\*", r".*"))

    children = _get_immediate_children_of_node(parent)

    res: List[str] = []
    for child in children:
        if not pattern_re.match(child):
            continue

        res.append(urljoins(parent, child))

    return res
