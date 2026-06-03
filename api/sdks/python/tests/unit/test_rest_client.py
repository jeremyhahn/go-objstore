"""Canonical unit tests for the REST (HTTP/JSON) client.

Implements the SDK canonical test matrix for the REST transport:

- success + error path for all 19 operations
- not_found path for the 9 designated operations
- metadata_round_trip
- validation_empty_key

The transport is mocked with the ``responses`` library; no live server is
needed. Test names follow ``test_rest_<op>_<variant>`` so coverage can be
diffed against the other SDKs at a glance.
"""

from __future__ import annotations

import gzip
import json

import pytest
import responses
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout

from objstore.exceptions import (
    ConnectionError,
    ObjectNotFoundError,
    ObjectStoreError,
    ServerError,
    TimeoutError,
    ValidationError,
)
from objstore.models import (
    LifecyclePolicy,
    Metadata,
    ReplicationPolicy,
    TriggerReplicationOptions,
)
from objstore.rest_client import RestClient


BASE = "http://localhost:8080"
API = f"{BASE}/api/v1"


def _client() -> RestClient:
    return RestClient(base_url=BASE, api_version="v1")


def _policy() -> LifecyclePolicy:
    return LifecyclePolicy(id="p1", prefix="x/", retention_seconds=10, action="delete")


def _repl() -> ReplicationPolicy:
    return ReplicationPolicy(
        id="r1",
        source_backend="local",
        destination_backend="s3",
        check_interval_seconds=30,
    )


def _opts() -> TriggerReplicationOptions:
    return TriggerReplicationOptions(policy_id="r1")


# =====================================================================
# put
# =====================================================================


@responses.activate
def test_rest_put_success() -> None:
    responses.add(responses.PUT, f"{API}/objects/k",
                  json={"message": "ok", "data": {"etag": "e1"}}, status=201)
    result = _client().put("k", b"data")
    assert result.success is True
    assert result.etag == "e1"


@responses.activate
def test_rest_put_error() -> None:
    responses.add(responses.PUT, f"{API}/objects/k",
                  json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().put("k", b"data")


# =====================================================================
# get
# =====================================================================


@responses.activate
def test_rest_get_success() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body=b"hello",
                  headers={"Content-Type": "text/plain", "Content-Length": "5",
                           "ETag": "e1"}, status=200)
    data, metadata = _client().get("k")
    assert data == b"hello"
    assert metadata.content_type == "text/plain"
    assert metadata.size == 5
    assert metadata.etag == "e1"


@responses.activate
def test_rest_get_error() -> None:
    responses.add(responses.GET, f"{API}/objects/k", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().get("k")


@responses.activate
def test_rest_get_not_found() -> None:
    responses.add(responses.GET, f"{API}/objects/k", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().get("k")


# =====================================================================
# delete
# =====================================================================


@responses.activate
def test_rest_delete_success() -> None:
    responses.add(responses.DELETE, f"{API}/objects/k", json={"message": "deleted"}, status=200)
    assert _client().delete("k").success is True


@responses.activate
def test_rest_delete_error() -> None:
    # A 500 with a non-not-found message surfaces as a ServerError.
    responses.add(responses.DELETE, f"{API}/objects/k",
                  json={"message": "internal failure"}, status=500)
    with pytest.raises(ServerError):
        _client().delete("k")


@responses.activate
def test_rest_delete_not_found() -> None:
    responses.add(responses.DELETE, f"{API}/objects/k", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().delete("k")


# =====================================================================
# list
# =====================================================================


@responses.activate
def test_rest_list_success() -> None:
    responses.add(responses.GET, f"{API}/objects", json={
        "objects": [{"key": "o1", "size": 1, "etag": "e1"},
                    {"key": "o2", "size": 2, "etag": "e2"}],
        "common_prefixes": ["d/"],
        "next_token": "tok",
        "truncated": True,
    }, status=200)
    result = _client().list(prefix="p", max_results=10)
    assert [o.key for o in result.objects] == ["o1", "o2"]
    assert result.next_token == "tok"
    assert result.truncated is True


@responses.activate
def test_rest_list_error() -> None:
    responses.add(responses.GET, f"{API}/objects", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().list()


# =====================================================================
# exists
# =====================================================================


@responses.activate
def test_rest_exists_success() -> None:
    responses.add(responses.HEAD, f"{API}/objects/k", status=200)
    assert _client().exists("k").exists is True


@responses.activate
def test_rest_exists_error() -> None:
    # exists() must raise on a server error (5xx); only 404 yields exists=False.
    responses.add(responses.HEAD, f"{API}/objects/k", status=500)
    with pytest.raises(ServerError):
        _client().exists("k")


@responses.activate
def test_rest_exists_not_found() -> None:
    responses.add(responses.HEAD, f"{API}/objects/k", status=404)
    assert _client().exists("k").exists is False


# =====================================================================
# get_metadata
# =====================================================================


@responses.activate
def test_rest_get_metadata_success() -> None:
    responses.add(responses.GET, f"{API}/metadata/k", json={
        "size": 100, "etag": "e1", "content_type": "text/plain",
        "metadata": {"author": "alice"},
    }, status=200)
    metadata = _client().get_metadata("k")
    assert metadata.size == 100
    assert metadata.content_type == "text/plain"
    assert metadata.custom == {"author": "alice"}


@responses.activate
def test_rest_get_metadata_error() -> None:
    responses.add(responses.GET, f"{API}/metadata/k", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().get_metadata("k")


@responses.activate
def test_rest_get_metadata_not_found() -> None:
    responses.add(responses.GET, f"{API}/metadata/k", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().get_metadata("k")


# =====================================================================
# update_metadata
# =====================================================================


@responses.activate
def test_rest_update_metadata_success() -> None:
    responses.add(responses.PUT, f"{API}/metadata/k", json={"message": "updated"}, status=200)
    result = _client().update_metadata("k", Metadata(content_type="application/json"))
    assert result.success is True


@responses.activate
def test_rest_update_metadata_error() -> None:
    responses.add(responses.PUT, f"{API}/metadata/k", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().update_metadata("k", Metadata())


@responses.activate
def test_rest_update_metadata_not_found() -> None:
    responses.add(responses.PUT, f"{API}/metadata/k", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().update_metadata("k", Metadata())


# =====================================================================
# health
# =====================================================================


@responses.activate
def test_rest_health_success() -> None:
    from objstore.models import HealthStatus

    responses.add(responses.GET, f"{BASE}/health",
                  json={"status": "SERVING", "message": "healthy"}, status=200)
    result = _client().health()
    assert result.status == HealthStatus.SERVING


@responses.activate
def test_rest_health_error() -> None:
    responses.add(responses.GET, f"{BASE}/health", json={"message": "down"}, status=500)
    with pytest.raises(ServerError):
        _client().health()


# =====================================================================
# archive
# =====================================================================


@responses.activate
def test_rest_archive_success() -> None:
    responses.add(responses.POST, f"{API}/archive", json={"message": "archived"}, status=200)
    assert _client().archive("k", "s3", {"bucket": "b"}).success is True


@responses.activate
def test_rest_archive_error() -> None:
    responses.add(responses.POST, f"{API}/archive", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().archive("k", "s3", {})


# =====================================================================
# add_policy
# =====================================================================


@responses.activate
def test_rest_add_policy_success() -> None:
    responses.add(responses.POST, f"{API}/policies", json={"message": "added"}, status=201)
    assert _client().add_policy(_policy()).success is True


@responses.activate
def test_rest_add_policy_error() -> None:
    responses.add(responses.POST, f"{API}/policies", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().add_policy(_policy())


# =====================================================================
# remove_policy
# =====================================================================


@responses.activate
def test_rest_remove_policy_success() -> None:
    responses.add(responses.DELETE, f"{API}/policies/p1", json={"message": "removed"}, status=200)
    assert _client().remove_policy("p1").success is True


@responses.activate
def test_rest_remove_policy_error() -> None:
    responses.add(responses.DELETE, f"{API}/policies/p1", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().remove_policy("p1")


@responses.activate
def test_rest_remove_policy_not_found() -> None:
    responses.add(responses.DELETE, f"{API}/policies/p1", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().remove_policy("p1")


# =====================================================================
# get_policies
# =====================================================================


@responses.activate
def test_rest_get_policies_success() -> None:
    responses.add(responses.GET, f"{API}/policies", json={
        "policies": [{"id": "p1", "prefix": "x/", "retention_seconds": 10, "action": "delete"}],
        "message": "ok",
    }, status=200)
    result = _client().get_policies()
    assert result.success is True
    assert len(result.policies) == 1


@responses.activate
def test_rest_get_policies_error() -> None:
    responses.add(responses.GET, f"{API}/policies", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().get_policies()


# =====================================================================
# apply_policies
# =====================================================================


@responses.activate
def test_rest_apply_policies_success() -> None:
    responses.add(responses.POST, f"{API}/policies/apply", json={
        "policies_count": 3, "objects_processed": 100, "message": "applied",
    }, status=200)
    result = _client().apply_policies()
    assert result.success is True
    assert result.policies_count == 3


@responses.activate
def test_rest_apply_policies_error() -> None:
    responses.add(responses.POST, f"{API}/policies/apply", json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().apply_policies()


# =====================================================================
# add_replication_policy
# =====================================================================


@responses.activate
def test_rest_add_replication_policy_success() -> None:
    responses.add(responses.POST, f"{API}/replication/policies",
                  json={"message": "added"}, status=201)
    assert _client().add_replication_policy(_repl()).success is True


@responses.activate
def test_rest_add_replication_policy_error() -> None:
    responses.add(responses.POST, f"{API}/replication/policies",
                  json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().add_replication_policy(_repl())


# =====================================================================
# remove_replication_policy
# =====================================================================


@responses.activate
def test_rest_remove_replication_policy_success() -> None:
    responses.add(responses.DELETE, f"{API}/replication/policies/r1",
                  json={"message": "removed"}, status=200)
    assert _client().remove_replication_policy("r1").success is True


@responses.activate
def test_rest_remove_replication_policy_error() -> None:
    responses.add(responses.DELETE, f"{API}/replication/policies/r1",
                  json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().remove_replication_policy("r1")


@responses.activate
def test_rest_remove_replication_policy_not_found() -> None:
    responses.add(responses.DELETE, f"{API}/replication/policies/r1", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().remove_replication_policy("r1")


# =====================================================================
# get_replication_policies
# =====================================================================


@responses.activate
def test_rest_get_replication_policies_success() -> None:
    responses.add(responses.GET, f"{API}/replication/policies", json={
        "policies": [{"id": "r1", "source_backend": "local",
                      "destination_backend": "s3", "check_interval_seconds": 30}],
    }, status=200)
    result = _client().get_replication_policies()
    assert len(result.policies) == 1


@responses.activate
def test_rest_get_replication_policies_error() -> None:
    responses.add(responses.GET, f"{API}/replication/policies",
                  json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().get_replication_policies()


# =====================================================================
# get_replication_policy
# =====================================================================


@responses.activate
def test_rest_get_replication_policy_success() -> None:
    # The server responds with a bare ReplicationPolicyResponse (no "policy" wrapper).
    responses.add(responses.GET, f"{API}/replication/policies/r1", json={
        "id": "r1", "source_backend": "local",
        "destination_backend": "s3", "check_interval_seconds": 30,
        "enabled": True, "replication_mode": "transparent",
    }, status=200)
    policy = _client().get_replication_policy("r1")
    assert policy.id == "r1"


@responses.activate
def test_rest_get_replication_policy_error() -> None:
    responses.add(responses.GET, f"{API}/replication/policies/r1",
                  json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().get_replication_policy("r1")


@responses.activate
def test_rest_get_replication_policy_not_found() -> None:
    responses.add(responses.GET, f"{API}/replication/policies/r1", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().get_replication_policy("r1")


# =====================================================================
# trigger_replication
# =====================================================================


@responses.activate
def test_rest_trigger_replication_success() -> None:
    responses.add(responses.POST, f"{API}/replication/trigger", json={
        "result": {"policy_id": "r1", "synced": 100, "deleted": 5, "failed": 0,
                   "bytes_total": 1048576, "duration_ms": 5000},
        "message": "triggered",
    }, status=200)
    result = _client().trigger_replication(_opts())
    assert result.success is True
    assert result.result.synced == 100


@responses.activate
def test_rest_trigger_replication_error() -> None:
    responses.add(responses.POST, f"{API}/replication/trigger",
                  json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().trigger_replication(_opts())


# =====================================================================
# get_replication_status
# =====================================================================


@responses.activate
def test_rest_get_replication_status_success() -> None:
    # The server responds with a bare ReplicationStatusResponse (no "status" wrapper).
    # The REST wire format uses average_sync_duration as a Go-duration string.
    responses.add(responses.GET, f"{API}/replication/status/r1", json={
        "policy_id": "r1", "source_backend": "local",
        "destination_backend": "s3", "enabled": True,
        "total_objects_synced": 1000, "total_objects_deleted": 10,
        "total_bytes_synced": 10485760, "total_errors": 0,
        "average_sync_duration": "2s", "sync_count": 5,
    }, status=200)
    result = _client().get_replication_status("r1")
    assert result.success is True
    assert result.status.total_objects_synced == 1000
    assert result.status.average_sync_duration_ms == 2000


@responses.activate
def test_rest_get_replication_status_error() -> None:
    responses.add(responses.GET, f"{API}/replication/status/r1",
                  json={"message": "boom"}, status=500)
    with pytest.raises(ServerError):
        _client().get_replication_status("r1")


@responses.activate
def test_rest_get_replication_status_not_found() -> None:
    responses.add(responses.GET, f"{API}/replication/status/r1", status=404)
    with pytest.raises(ObjectNotFoundError):
        _client().get_replication_status("r1")


# =====================================================================
# metadata_round_trip
# =====================================================================


@responses.activate
def test_rest_metadata_round_trip() -> None:
    """content_type, content_encoding and the custom map survive put -> get.

    Wire scheme: PUT sets Content-Type, Content-Encoding and X-Object-Metadata
    (JSON of the custom map only); the GET response returns the same via the
    matching headers.
    """
    custom = {"author": "carol", "project": "objstore"}

    responses.add(responses.PUT, f"{API}/objects/rt",
                  json={"message": "ok", "data": {"etag": "rt"}}, status=201)
    # The GET body is sent uncompressed (no Content-Encoding header) so the
    # `responses` library does not attempt to gunzip a plain body. The
    # content_encoding round trip is asserted on the metadata endpoint, whose
    # body IS gzip-compressed so the matching header is valid.
    responses.add(responses.GET, f"{API}/objects/rt", body=b"payload",
                  headers={"Content-Type": "application/json",
                           "Content-Length": "7",
                           "ETag": "rt",
                           "X-Object-Metadata": json.dumps(custom)},
                  status=200)
    meta_body = gzip.compress(json.dumps(
        {"size": 7, "etag": "rt", "content_type": "application/json",
         "metadata": custom}).encode())
    responses.add(responses.GET, f"{API}/metadata/rt", body=meta_body,
                  headers={"Content-Type": "application/json",
                           "Content-Encoding": "gzip",
                           "X-Object-Metadata": json.dumps(custom)},
                  status=200)

    client = _client()
    client.put("rt", b"payload",
               metadata=Metadata(content_type="application/json",
                                 content_encoding="gzip", custom=custom))

    # PUT request carries the three pieces; X-Object-Metadata is custom only.
    sent = responses.calls[0].request.headers
    assert sent["Content-Type"] == "application/json"
    assert sent["Content-Encoding"] == "gzip"
    assert json.loads(sent["X-Object-Metadata"]) == custom

    _, meta = client.get("rt")
    assert meta.content_type == "application/json"
    assert meta.custom == custom

    md = client.get_metadata("rt")
    assert md.content_encoding == "gzip"
    assert md.custom == custom


# =====================================================================
# validation_empty_key
# =====================================================================


@responses.activate
def test_rest_validation_empty_key() -> None:
    """An empty key is rejected by the server (HTTP 400 -> ValidationError).

    The client builds ``.../objects/`` for an empty key; match the URL with a
    regex so trailing-slash normalisation cannot cause a miss.
    """
    import re

    responses.add(responses.GET, re.compile(rf"{re.escape(API)}/objects/?$"),
                  json={"message": "key must not be empty"}, status=400)
    with pytest.raises(ValidationError):
        _client().get("")


# =====================================================================
# language-specific extras: construction, streaming, transport failures
# =====================================================================


def test_rest_url_construction() -> None:
    client = _client()
    assert client._url("objects/k") == f"{API}/objects/k"
    assert client._url("/objects/k") == f"{API}/objects/k"


def test_rest_url_without_api_version() -> None:
    client = RestClient(base_url=BASE, api_version="")
    assert client._url("health") == f"{BASE}/health"


def test_rest_context_manager() -> None:
    with _client() as client:
        assert client is not None


@responses.activate
def test_rest_get_stream_success() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body=b"chunk1chunk2", status=200)
    chunks = list(_client().get_stream("k"))
    assert b"".join(chunks) == b"chunk1chunk2"


@responses.activate
def test_rest_get_stream_not_found() -> None:
    responses.add(responses.GET, f"{API}/objects/k", status=404)
    with pytest.raises(ObjectNotFoundError):
        list(_client().get_stream("k"))


@responses.activate
def test_rest_authentication_error() -> None:
    from objstore.exceptions import AuthenticationError

    responses.add(responses.GET, f"{API}/objects/k", status=401)
    with pytest.raises(AuthenticationError):
        _client().get("k")


@responses.activate
def test_rest_generic_error_code() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body="teapot", status=418)
    with pytest.raises(ObjectStoreError):
        _client().get("k")


def test_rest_timeout_branch() -> None:
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{API}/objects/k", body=Timeout())
        with pytest.raises(TimeoutError):
            _client().get("k")


def test_rest_connection_error_branch() -> None:
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{API}/objects/k", body=RequestsConnectionError())
        with pytest.raises(ConnectionError):
            _client().get("k")


def test_rest_health_timeout() -> None:
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{BASE}/health", body=Timeout())
        with pytest.raises(TimeoutError):
            _client().health()


# =====================================================================
# transport-failure sweep (timeout + connection) for every operation
#
# The canonical matrix asserts one success + one error per op; this sweep
# additionally drives the per-method requests.Timeout / ConnectionError
# handlers so the client's transport-error mapping is fully covered.
# =====================================================================


def _invocations(client: RestClient):
    return [
        (responses.PUT, "objects/k", lambda: client.put("k", b"d")),
        (responses.GET, "objects/k", lambda: client.get("k")),
        (responses.GET, "objects/k", lambda: list(client.get_stream("k"))),
        (responses.DELETE, "objects/k", lambda: client.delete("k")),
        (responses.GET, "objects", lambda: client.list()),
        (responses.HEAD, "objects/k", lambda: client.exists("k")),
        (responses.GET, "metadata/k", lambda: client.get_metadata("k")),
        (responses.PUT, "metadata/k", lambda: client.update_metadata("k", Metadata())),
        (responses.POST, "archive", lambda: client.archive("k", "s3", {})),
        (responses.POST, "policies", lambda: client.add_policy(_policy())),
        (responses.DELETE, "policies/p1", lambda: client.remove_policy("p1")),
        (responses.GET, "policies", lambda: client.get_policies()),
        (responses.POST, "policies/apply", lambda: client.apply_policies()),
        (responses.POST, "replication/policies",
         lambda: client.add_replication_policy(_repl())),
        (responses.DELETE, "replication/policies/r1",
         lambda: client.remove_replication_policy("r1")),
        (responses.GET, "replication/policies",
         lambda: client.get_replication_policies()),
        (responses.GET, "replication/policies/r1",
         lambda: client.get_replication_policy("r1")),
        (responses.POST, "replication/trigger",
         lambda: client.trigger_replication(_opts())),
        (responses.GET, "replication/status/r1",
         lambda: client.get_replication_status("r1")),
    ]


def test_rest_timeout_branch_for_every_method() -> None:
    client = _client()
    for http_method, suffix, call in _invocations(client):
        with responses.RequestsMock() as rsps:
            rsps.add(http_method, f"{API}/{suffix}", body=Timeout())
            with pytest.raises(TimeoutError):
                call()


def test_rest_connection_error_branch_for_every_method() -> None:
    client = _client()
    for http_method, suffix, call in _invocations(client):
        with responses.RequestsMock() as rsps:
            rsps.add(http_method, f"{API}/{suffix}", body=RequestsConnectionError())
            with pytest.raises(ConnectionError):
                call()


def test_rest_health_connection_error() -> None:
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, f"{BASE}/health", body=RequestsConnectionError())
        with pytest.raises(ConnectionError):
            _client().health()


# ---- _handle_error and header-parsing edge cases --------------------


@responses.activate
def test_rest_validation_error_non_json_body() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body="bad request text", status=400)
    with pytest.raises(ValidationError):
        _client().get("k")


@responses.activate
def test_rest_server_error_non_json_body() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body="server exploded", status=500)
    with pytest.raises(ServerError):
        _client().get("k")


@responses.activate
def test_rest_delete_500_not_found_message() -> None:
    # The client treats a 500 whose message says "not found" as ObjectNotFound.
    responses.add(responses.DELETE, f"{API}/objects/k",
                  json={"message": "object not found"}, status=500)
    with pytest.raises(ObjectNotFoundError):
        _client().delete("k")


@responses.activate
def test_rest_delete_500_non_json_body() -> None:
    responses.add(responses.DELETE, f"{API}/objects/k", body="plain text", status=500)
    with pytest.raises(ServerError):
        _client().delete("k")


@responses.activate
def test_rest_get_custom_header_malformed_json() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body=b"d",
                  headers={"Content-Type": "text/plain", "Content-Length": "1",
                           "X-Object-Metadata": "{not valid json"}, status=200)
    _, meta = _client().get("k")
    assert meta.custom == {}


@responses.activate
def test_rest_get_custom_header_non_object_json() -> None:
    responses.add(responses.GET, f"{API}/objects/k", body=b"d",
                  headers={"Content-Type": "text/plain", "Content-Length": "1",
                           "X-Object-Metadata": "[1, 2, 3]"}, status=200)
    _, meta = _client().get("k")
    assert meta.custom == {}


@responses.activate
def test_rest_put_file_like_object() -> None:
    from io import BytesIO

    responses.add(responses.PUT, f"{API}/objects/k",
                  json={"message": "ok", "data": {"etag": "e"}}, status=201)
    result = _client().put("k", BytesIO(b"file bytes"))
    assert result.success is True
    assert responses.calls[0].request.body == b"file bytes"


@responses.activate
def test_rest_update_metadata_201_treated_as_success() -> None:
    responses.add(responses.PUT, f"{API}/metadata/k", json={"message": "created"}, status=201)
    assert _client().update_metadata("k", Metadata()).success is True


@responses.activate
def test_rest_health_unknown_status() -> None:
    from objstore.models import HealthStatus

    responses.add(responses.GET, f"{BASE}/health", json={"status": "WAT"}, status=200)
    assert _client().health().status == HealthStatus.UNKNOWN
