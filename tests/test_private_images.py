from collections.abc import Callable
from pathlib import Path
from typing import Any, Optional

import click
import graphql
import pytest

from latch_cli.services import private_images
from latch_cli.services.docker import utils as docker_utils
from latch_cli.services.private_images import (
    PrivateImages,
    is_recorded_in_db,
    record_failed_exit_code,
    record_in_db,
    record_in_db_or_exit,
    resolve_pull_reference,
    resolve_workspace_id,
)

from .conftest import ACTIVE_WS, OTHER_WS, PASSWORD, _MissingImageClient

# one workspace for the whole module, so the stubs and the assertions agree
WS_ID = ACTIVE_WS
IMAGE_NAME = f"{ACTIVE_WS}_barcode_tools"
VERSION = "abc123"

RECORDED_NODE = {"workspaceId": WS_ID, "imageName": IMAGE_NAME, "version": VERSION}
OTHER_WORKSPACE_NODE = {
    "workspaceId": "9999",
    "imageName": IMAGE_NAME,
    "version": VERSION,
}


def fake_execute(
    response: Optional[dict[str, Any]], calls: list[tuple[str, Any]]
) -> Callable[..., dict[str, Any]]:
    """Return a stand-in for `execute` that records the document and its variables."""

    def execute(
        document: graphql.DocumentNode,
        variables: Optional[dict[str, Any]] = None,
        **_kwargs: object,
    ) -> dict[str, Any]:
        calls.append((graphql.print_ast(document), variables))

        return {"privateImages": response}

    return execute


OTHER_IMAGE_NODE = {
    "workspaceId": WS_ID,
    "imageName": f"{ACTIVE_WS}_other_tool",
    "version": VERSION,
}
OTHER_VERSION_NODE = {
    "workspaceId": WS_ID,
    "imageName": IMAGE_NAME,
    "version": "zzz999",
}


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"nodes": [RECORDED_NODE]}, True),
        ({"nodes": [OTHER_IMAGE_NODE]}, False),
        ({"nodes": [OTHER_VERSION_NODE]}, False),
        ({"nodes": [OTHER_WORKSPACE_NODE]}, False),
        ({"nodes": [OTHER_IMAGE_NODE, RECORDED_NODE]}, True),
        ({"nodes": []}, False),
        ({"nodes": None}, False),
        (None, False),
    ],
)
def test_is_recorded_in_db(
    monkeypatch: pytest.MonkeyPatch,
    response: Optional[dict[str, Any]],
    *,
    expected: bool,
):
    """Only a node matching both the image and the version counts as recorded."""
    monkeypatch.setattr(private_images, "execute", fake_execute(response, []))

    assert is_recorded_in_db(WS_ID, IMAGE_NAME, VERSION) is expected


def test_record_in_db_creates_the_record_despite_an_unrelated_node(
    monkeypatch: pytest.MonkeyPatch,
):
    """The create must still run when the workspace holds only other images."""
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [OTHER_IMAGE_NODE]}, calls)
    )

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 2
    assert "createPrivateImage" in calls[1][0]


def test_record_in_db_skips_the_mutation_when_already_recorded(
    monkeypatch: pytest.MonkeyPatch,
):
    """A retry of a partly completed upload must not hit the uniqueness constraint."""
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [RECORDED_NODE]}, calls)
    )

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 1
    assert "createPrivateImage" not in calls[0][0]


def test_record_in_db_creates_the_record_when_missing(monkeypatch: pytest.MonkeyPatch):
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(private_images, "execute", fake_execute({"nodes": []}, calls))

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 2
    assert "createPrivateImage" in calls[1][0]


def test_record_in_db_or_exit_is_silent_on_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    monkeypatch.setattr(
        private_images, "record_in_db", lambda _ws_id, _image_name, _version: None
    )

    record_in_db_or_exit(WS_ID, IMAGE_NAME, VERSION, full_image_ref="ecr/image:abc123")

    assert capsys.readouterr().out == ""


def test_record_in_db_or_exit_uses_a_distinct_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """A record failure must not look like a push failure, which exits 1."""

    def raises(_ws_id: str, _image_name: str, _version: str) -> None:
        raise RuntimeError("uniqueness violation")

    monkeypatch.setattr(private_images, "record_in_db", raises)

    full_image_ref = f"ecr/{IMAGE_NAME}:{VERSION}"

    with pytest.raises(click.exceptions.Exit) as excinfo:
        record_in_db_or_exit(WS_ID, IMAGE_NAME, VERSION, full_image_ref=full_image_ref)

    # the literal is the promise to callers; asserting the constant is self-referential
    assert excinfo.value.exit_code == 3
    assert record_failed_exit_code == 3

    out = capsys.readouterr().out
    assert full_image_ref in out
    assert "uniqueness violation" in out


def test_record_in_db_or_exit_passes_through_a_click_exit(
    monkeypatch: pytest.MonkeyPatch,
):
    """A click exit from below must keep its own code, not be relabelled as 3."""

    def raises(_ws_id: str, _image_name: str, _version: str) -> None:
        raise click.exceptions.Exit(1)

    monkeypatch.setattr(private_images, "record_in_db", raises)

    with pytest.raises(click.exceptions.Exit) as excinfo:
        record_in_db_or_exit(
            WS_ID, IMAGE_NAME, VERSION, full_image_ref="ecr/image:abc123"
        )

    assert excinfo.value.exit_code == 1


def test_record_in_db_or_exit_passes_through_a_click_abort(
    monkeypatch: pytest.MonkeyPatch,
):
    """A click abort from below must propagate, not be relabelled as 3."""

    def raises(_ws_id: str, _image_name: str, _version: str) -> None:
        raise click.Abort

    monkeypatch.setattr(private_images, "record_in_db", raises)

    with pytest.raises(click.Abort):
        record_in_db_or_exit(
            WS_ID, IMAGE_NAME, VERSION, full_image_ref="ecr/image:abc123"
        )


def test_record_failure_message_survives_a_multiline_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """A multi-line error must not defeat the dedent and skew the whole message."""

    def raises(_ws_id: str, _image_name: str, _version: str) -> None:
        raise RuntimeError("Transport error:\n{'message': 'duplicate key'}")

    monkeypatch.setattr(private_images, "record_in_db", raises)

    with pytest.raises(click.exceptions.Exit):
        record_in_db_or_exit(
            WS_ID, IMAGE_NAME, VERSION, full_image_ref="ecr/image:abc123"
        )

    out = capsys.readouterr().out
    assert out.startswith("The image reached the registry")
    # every line of the template is flush left; only the error body is indented
    assert "\n`ecr/image:abc123` is pushed" in out


def test_is_recorded_in_db_sends_all_three_variables(monkeypatch: pytest.MonkeyPatch):
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [RECORDED_NODE]}, calls)
    )

    is_recorded_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert calls[0][1] == {"wsId": WS_ID, "imageName": IMAGE_NAME, "version": VERSION}


@pytest.mark.parametrize(
    ("given", "resolved"), [(None, ACTIVE_WS), (OTHER_WS, OTHER_WS)]
)
@pytest.mark.usefixtures("_workspaces")
def test_resolve_workspace_id(given: Optional[str], resolved: str):
    """An explicit id wins over the ambient one, or the two can disagree."""
    assert resolve_workspace_id(given) == resolved


@pytest.mark.usefixtures("_workspaces")
def test_resolve_workspace_id_names_an_explicit_target(
    capsys: pytest.CaptureFixture[str],
):
    resolve_workspace_id(OTHER_WS)

    out = capsys.readouterr().out
    assert "Other Team" in out
    assert OTHER_WS in out


@pytest.mark.usefixtures("_workspaces")
def test_resolve_workspace_id_rejects_an_unreachable_workspace(
    capsys: pytest.CaptureFixture[str],
):
    """A typo must fail before the push, not push into someone else's namespace."""

    with pytest.raises(click.exceptions.Exit) as excinfo:
        resolve_workspace_id("9999")

    assert excinfo.value.exit_code == 1
    assert "9999" in capsys.readouterr().out


def test_resolve_workspace_id_does_not_look_up_the_active_workspace(
    monkeypatch: pytest.MonkeyPatch,
):
    """A deliberate difference from `latch register`, which checks both paths.

    The active workspace comes from the user's own config, so it is not worth a
    `get_workspaces` query on every `ls`. If it is wrong, `get_credentials` fails.
    Change this only together with the docstring on `resolve_workspace_id`.
    """

    def unreachable() -> dict[str, object]:
        raise AssertionError("get_workspaces must not be called for the default")

    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)
    monkeypatch.setattr(private_images, "get_workspaces", unreachable)

    assert resolve_workspace_id(None) == ACTIVE_WS


class _FakeDockerClient:
    """Minimal stand-in for the pieces of `docker.APIClient` that `upload_image` uses."""

    def __init__(self) -> None:
        self.tagged: dict[str, str] = {}
        self.pushed: dict[str, str] = {}
        self._auth_configs: object = None

    @staticmethod
    def inspect_image(image_ref: str) -> dict[str, str]:
        return {"Id": image_ref}

    def tag(self, _image_ref: str, *, repository: str, tag: str) -> bool:
        self.tagged = {"repository": repository, "tag": tag}
        return True

    def push(self, *, repository: str, tag: str, **_kwargs: object) -> list[object]:
        self.pushed = {"repository": repository, "tag": tag}
        return []


@pytest.mark.parametrize("remote", [True, False])
@pytest.mark.usefixtures("_workspaces")
def test_build_and_upload_passes_the_workspace_to_the_build(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *, remote: bool
):
    """Both build paths must be handed the resolved workspace, not resolve their own."""
    seen: dict[str, str] = {}

    def build(*_args: object, ws_id: str, **_kwargs: object) -> None:
        seen["ws_id"] = ws_id

    monkeypatch.setattr(private_images, "remote_dbnp", build)
    monkeypatch.setattr(private_images, "dbnp", build)
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: None)
    monkeypatch.setattr(
        private_images, "record_in_db_or_exit", lambda *_args, **_kwargs: None
    )

    (tmp_path / "Dockerfile").touch()

    private_images.build_and_upload_image(
        tmp_path,
        image_name="image",
        version="v1",
        workspace_id=OTHER_WS,
        remote=remote,
        skip_confirmation=True,
    )

    assert seen["ws_id"] == OTHER_WS


@pytest.mark.parametrize("workspace_id", [None, OTHER_WS])
@pytest.mark.usefixtures("_workspaces")
def test_ls_accepts_a_workspace(
    monkeypatch: pytest.MonkeyPatch, workspace_id: Optional[str]
):
    seen: list[str] = []

    def execute(
        _document: object, variables: dict[str, str], **_kwargs: object
    ) -> dict:
        seen.append(variables["wsId"])
        return {"privateImages": {"nodes": []}}

    monkeypatch.setattr(private_images, "execute", execute)

    private_images.ls(workspace_id=workspace_id)

    assert seen == [workspace_id if workspace_id is not None else ACTIVE_WS]


@pytest.mark.usefixtures("_workspaces")
def test_upload_image_uses_the_workspace_for_every_consumer(
    monkeypatch: pytest.MonkeyPatch,
):
    """The repository name, the credentials and the record must agree on one workspace.

    `upload_image` is the command the workspace flag exists for, so drive it end to
    end rather than testing the pieces separately.
    """
    credentials_call: dict[str, str] = {}
    recorded: dict[str, str] = {}
    client = _FakeDockerClient()

    def get_credentials(image: str, *, ws_id: str) -> object:
        credentials_call.update({"image": image, "ws_id": ws_id})
        return docker_utils.DockerCredentials(username="u", password=PASSWORD)

    def record(ws_id: str, image_name: str, version: str, **_kwargs: object) -> None:
        recorded.update({"ws_id": ws_id, "image_name": image_name, "version": version})

    monkeypatch.setattr(private_images, "get_credentials", get_credentials)
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: client)
    monkeypatch.setattr(private_images, "print_upload_logs", lambda *_a, **_k: None)
    monkeypatch.setattr(private_images, "record_in_db_or_exit", record)

    private_images.upload_image(
        "some_registry/image:v1", workspace_id=OTHER_WS, skip_confirmation=True
    )

    namespaced = f"{OTHER_WS}_image"

    assert credentials_call == {"image": namespaced, "ws_id": OTHER_WS}
    assert client.tagged["repository"].endswith(f"/{namespaced}")
    assert client.pushed["repository"].endswith(f"/{namespaced}")
    assert recorded == {"ws_id": OTHER_WS, "image_name": namespaced, "version": "v1"}


@pytest.mark.parametrize(
    ("image_ref", "resolved"),
    [
        # unqualified: Docker Hub, in a namespace the caller may not own
        ("team/tool:v1", "docker.io/team/tool:v1"),
        ("team/tool", "docker.io/team/tool"),
        # single component: Docker Hub's official-image namespace
        ("ubuntu:22.04", "docker.io/library/ubuntu:22.04"),
        # a real registry is left alone
        ("ghcr.io/team/tool:v1", "ghcr.io/team/tool:v1"),
        ("ghcr.io/team/sub/tool:v1", "ghcr.io/team/sub/tool:v1"),
        ("localhost/tool:v1", "localhost/tool:v1"),
        # a port in the first component marks it a host, even without a dot
        ("localhost:5000/tool:v1", "localhost:5000/tool:v1"),
        # a digest reference keeps its digest
        ("team/tool@sha256:abc", "docker.io/team/tool@sha256:abc"),
        ("ubuntu@sha256:abc", "docker.io/library/ubuntu@sha256:abc"),
        # uppercase cannot appear in a repository path, so it can only be a host
        ("MyOrg/tool:v1", "MyOrg/tool:v1"),
    ],
)
def test_resolve_pull_reference(image_ref: str, resolved: str):
    """A host-looking first component is a registry; anything else is Docker Hub."""
    assert resolve_pull_reference(image_ref) == resolved


@pytest.mark.usefixtures("_upload_stubs", "_quiet_upload_logs")
def test_missing_image_does_not_pull_by_default(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """Publishing a third-party image under our tag must not happen silently."""
    client = _MissingImageClient()
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: client)

    with pytest.raises(click.exceptions.Exit) as excinfo:
        private_images.upload_image("team/tool:v1", skip_confirmation=True)

    assert excinfo.value.exit_code == 1
    assert client.pulled == []
    # the invariant, not its proxy: nothing reached our registry
    assert client.pushed == []

    out = capsys.readouterr().out
    assert "--pull" in out
    # the message names where the image would actually come from
    assert "docker.io/team/tool:v1" in out


@pytest.mark.usefixtures("_upload_stubs", "_quiet_upload_logs")
def test_pull_flag_pulls_the_image(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    client = _MissingImageClient()
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: client)

    private_images.upload_image(
        "team/tool:v1", should_pull=True, skip_confirmation=True
    )

    # we pull the reference we printed, so the message cannot drift from the call
    assert client.pulled == ["docker.io/team/tool:v1"]
    assert "docker.io/team/tool:v1" in capsys.readouterr().out


class _LocalImageClient(_MissingImageClient):
    @staticmethod
    def inspect_image(image_ref: str) -> dict[str, str]:
        return {"Id": image_ref}


@pytest.mark.usefixtures("_upload_stubs", "_quiet_upload_logs")
def test_a_local_image_never_pulls(monkeypatch: pytest.MonkeyPatch):
    """The flag only affects the missing-image path."""
    client = _LocalImageClient()
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: client)

    private_images.upload_image("team/tool:v1", skip_confirmation=True)

    assert client.pulled == []
    assert client.pushed == [f"{private_images.ecr_base}/{WS_ID}_tool"]


def _stub_ls_response(
    monkeypatch: pytest.MonkeyPatch, response: Optional[PrivateImages]
) -> list[tuple[str, Any]]:
    """Stub `execute` for `ls` and return the list that records its calls."""
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)
    monkeypatch.setattr(private_images, "execute", fake_execute(response, calls))

    return calls


def test_ls_exits_0_on_an_empty_workspace(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """An empty workspace must be distinguishable from an auth or network failure."""
    _stub_ls_response(monkeypatch, {"nodes": []})

    # returning rather than raising is the exit-0 contract
    assert private_images.ls() is None

    captured = capsys.readouterr()
    # nothing on stdout, so a caller can parse it without special-casing empty
    assert captured.out == ""
    assert "No private images" in captured.err


@pytest.mark.parametrize("response", [None, {"nodes": None}])
def test_ls_exits_1_when_the_workspace_cannot_be_read(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    response: Optional[PrivateImages],
):
    """A null connection or a null node list is a failed read, not an empty workspace.

    Postgraphile returns a non-null `nodes` whenever the connection is non-null, so
    either null is a malformed response.
    """
    _stub_ls_response(monkeypatch, response)

    with pytest.raises(click.exceptions.Exit) as excinfo:
        private_images.ls()

    assert excinfo.value.exit_code == 1

    captured = capsys.readouterr()
    # the failure must not land in the stream a caller redirects to a file
    assert captured.out == ""
    assert "Could not read" in captured.err


@pytest.mark.usefixtures("_workspaces")
def test_ls_queries_the_given_workspace(monkeypatch: pytest.MonkeyPatch):
    calls = _stub_ls_response(monkeypatch, {"nodes": []})

    private_images.ls(workspace_id=OTHER_WS)

    assert calls[0][1] == {"wsId": OTHER_WS}


def test_ls_lists_images_on_stdout(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """The listing itself goes to stdout, where a caller can read it."""
    _stub_ls_response(
        monkeypatch,
        {
            "nodes": [
                {
                    "imageName": IMAGE_NAME,
                    "version": VERSION,
                    "creationTime": "2026-08-12T00:00:00Z",
                }
            ]
        },
    )

    private_images.ls()

    captured = capsys.readouterr()
    assert f"{IMAGE_NAME}:{VERSION}" in captured.out
    # the listing must not leak a spurious note to the stream a caller redirects
    assert captured.err == ""


class _DigestPushClient(_MissingImageClient):
    """A daemon with the image present, whose push reports a digest."""

    digest = "sha256:8f1a2b3c4d5e6f70819293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8"

    @staticmethod
    def inspect_image(image_ref: str) -> dict[str, str]:
        return {"Id": image_ref}

    def push(self, **kwargs: object) -> list[object]:
        self.pushed.append(kwargs.get("repository"))
        return [{"aux": {"Digest": self.digest}}]


class _NoDigestPushClient(_DigestPushClient):
    def push(self, **kwargs: object) -> list[object]:
        self.pushed.append(kwargs.get("repository"))
        return [{"id": "layer_a", "progress": "1/1"}]


@pytest.mark.usefixtures("_upload_stubs")
def test_upload_reports_the_pushed_digest(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    client = _DigestPushClient()
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: client)

    private_images.upload_image("team/tool:v1", skip_confirmation=True)

    captured = capsys.readouterr()
    assert client.digest in captured.out
    assert "did not report a digest" not in captured.err


@pytest.mark.usefixtures("_upload_stubs")
def test_upload_says_so_when_no_digest_is_reported(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """Success without a digest is a weaker claim, and must not be stated as a strong one."""
    client = _NoDigestPushClient()
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: client)

    private_images.upload_image("team/tool:v1", skip_confirmation=True)

    captured = capsys.readouterr()
    assert "did not report a digest" in captured.err
    # the push itself did succeed, so the success line still stands
    assert "Successfully pushed" in captured.out
