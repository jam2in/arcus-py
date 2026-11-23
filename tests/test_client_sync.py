import socket
from collections.abc import Iterator

import pytest
from pytest_mock import MockerFixture, MockType
from support.fake_discovery import FakeDiscovery
from support.fake_server import FakeServer

import arcus
from arcus import (
    Arcus,
    ArcusClientError,
    ArcusConnectionError,
    ArcusError,
    ArcusErrorGroup,
    ArcusServerError,
    VectorOptions,
)
from arcus._cluster.locator import ArcusLocator

ZK_HOSTS = "127.0.0.1:2181"
SERVICE_CODE = "test"


@pytest.fixture
def server() -> Iterator[FakeServer]:
    with FakeServer() as server:
        yield server


@pytest.fixture
def discovery_class(mocker: MockerFixture) -> MockType:
    return mocker.patch("arcus._transport.sync.cluster.ArcusDiscovery", autospec=True)


def serve(discovery_class: MockType, *servers: FakeServer) -> FakeDiscovery:
    discovery = FakeDiscovery([server.address for server in servers])
    discovery_class.return_value = discovery
    return discovery


def test_arcus_is_exported() -> None:
    assert arcus.Arcus is Arcus
    assert "Arcus" in arcus.__all__


def test_invalid_config_is_rejected_on_creation() -> None:
    # Given / When / Then: An empty service code fails before anything starts.
    with pytest.raises(ValueError, match="service_code"):
        Arcus(ZK_HOSTS, "")


def test_creation_and_with_start_nothing(discovery_class: MockType) -> None:
    # Given / When: A client is created and used as a context manager.
    with Arcus(ZK_HOSTS, SERVICE_CODE):
        pass

    # Then: Discovery was never started.
    discovery_class.assert_not_called()


def test_set_then_get(server: FakeServer, discovery_class: MockType) -> None:
    # Given: A client for a service with one cache node.
    serve(discovery_class, server)

    with Arcus(ZK_HOSTS, SERVICE_CODE, zk_timeout=3.0) as client:
        # When: A value is stored and read back.
        stored = client.set("k", {"a": [1, 2]})
        value = client.get("k")
        missing = client.get("other")

    # Then: The value round-trips and discovery got the client settings.
    assert stored is True
    assert value == {"a": [1, 2]}
    assert missing is None
    discovery_class.assert_called_once_with(
        zk_hosts=ZK_HOSTS,
        service_code=SERVICE_CODE,
        timeout=3.0,
    )
    assert server.connections == 1


def test_vop_create_returns_a_handle_once(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client for a service with one cache node.
    serve(discovery_class, server)

    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        # When: The same vector index is created twice.
        options = VectorOptions(dim=3)
        created = client.vop_create("idx", options)
        again = client.vop_create("idx", options)

    # Then: Only the first create returns a handle on the index.
    assert created is not None
    assert created.name == "idx"
    assert again is None


def test_vop_upsert_through_the_client_and_the_handle(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client with one vector index created.
    serve(discovery_class, server)

    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        vector = client.vop_create("idx", VectorOptions(dim=3))
        assert vector is not None

        # When: Elements are upserted by index name, by handle, and to a missing index.
        by_name = client.vop_upsert("idx", "a", [1.0, 2.0, 3.0], meta={"n": 1})
        by_handle = vector.upsert("b", [1.0, 2.0, 3.0])
        missing = client.vop_upsert("nope", "a", [1.0, 2.0, 3.0])

    # Then: Only the upserts to the existing index are stored.
    assert by_name is True
    assert by_handle is True
    assert missing is False


def test_vop_search_by_vector_and_by_vkey(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client with a vector index holding one element with metadata.
    serve(discovery_class, server)

    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        vector = client.vop_create("idx", VectorOptions(dim=3))
        assert vector is not None
        vector.upsert("a", [1.0, 2.0, 3.0], meta={"n": 1})

        # When: The index is searched by a vector, by a vkey, and on a missing index.
        by_vector = client.vop_search("idx", [1.0, 2.0, 3.0], 5, filter="n = 1")
        by_vkey = vector.search("a", 5, with_meta=True)
        missing = client.vop_search("nope", "a", 5)

    # Then: Both searches find the element, with its metadata when asked for.
    assert by_vector == [("a", 0.0, None)]
    assert by_vkey == [("a", 0.0, {"n": 1})]
    assert missing is None


def test_vop_delete_through_the_client_and_the_handle(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client with a vector index holding two elements.
    serve(discovery_class, server)

    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        vector = client.vop_create("idx", VectorOptions(dim=3))
        assert vector is not None
        vector.upsert("a", [1.0, 2.0, 3.0])
        vector.upsert("b", [1.0, 2.0, 3.0])

        # When: Elements are deleted by index name, by handle, again, and on a missing
        # index.
        by_name = client.vop_delete("idx", "a")
        by_handle = vector.delete("b")
        again = vector.delete("a")
        missing = client.vop_delete("nope", "a")
        left = vector.search([1.0, 2.0, 3.0], 5)

    # Then: Only the deletes of present elements succeed, and none is left.
    assert by_name is True
    assert by_handle is True
    assert again is False
    assert missing is False
    assert left == []


def test_vop_drop_removes_the_index(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client with one vector index created.
    serve(discovery_class, server)

    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        vector = client.vop_create("idx", VectorOptions(dim=3))
        assert vector is not None

        # When: The index is dropped by handle, then again by name.
        dropped = vector.drop()
        again = client.vop_drop("idx")
        searched = client.vop_search("idx", [1.0, 2.0, 3.0], 5)
        recreated = client.vop_create("idx", VectorOptions(dim=3))

    # Then: Only the first drop succeeds, and the name is free again.
    assert dropped is True
    assert again is False
    assert searched is None
    assert recreated is not None


def test_key_is_stored_on_its_owner_node(discovery_class: MockType) -> None:
    key = "가".encode()

    with FakeServer() as first, FakeServer() as second:
        # Given: A service with two cache nodes.
        serve(discovery_class, first, second)
        owner = ArcusLocator([first.address, second.address]).get_node(key)

        # When: A value is stored under a non-ASCII key.
        with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
            client.set("가", "v")

    # Then: Only the node that owns the UTF-8 key holds it.
    holders = [s.address for s in (first, second) if key in s.items]
    assert holders == [owner]


def test_flush_invalidates_items_on_every_node(discovery_class: MockType) -> None:
    with FakeServer() as first, FakeServer() as second:
        # Given: A service with two cache nodes that both hold items.
        serve(discovery_class, first, second)
        keys = [f"k{i}" for i in range(20)]

        with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
            for key in keys:
                client.set(key, "v")
            assert first.items
            assert second.items

            # When: Every item is flushed.
            flushed = client.flush()
            values = [client.get(key) for key in keys]

    # Then: No item is left on either node.
    assert flushed is True
    assert values == [None] * len(keys)


def test_flush_with_prefix_invalidates_only_that_prefix(
    discovery_class: MockType,
) -> None:
    with FakeServer() as first, FakeServer() as second:
        # Given: Two nodes holding items of two prefixes and without one.
        serve(discovery_class, first, second)

        with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
            client.set("a:1", "v")
            client.set("b:1", "v")
            client.set("plain", "v")

            # When: One prefix, then the items without a prefix, are flushed.
            flushed = client.flush("a")
            after_prefix = [client.get(k) for k in ("a:1", "b:1", "plain")]
            client.flush("", delay=0)
            after_null = [client.get(k) for k in ("b:1", "plain")]
            flushed_again = client.flush("a")

    # Then: Each flush leaves the items of other prefixes, and a prefix
    # with no items left reports that nothing was flushed.
    assert flushed is True
    assert flushed_again is False
    assert after_prefix == [None, "v", "v"]
    assert after_null == ["v", None]


def test_flush_reaches_live_nodes_past_an_unreachable_one(
    discovery_class: MockType,
) -> None:
    # Given: A live node holding items, and a listed node that refuses connections.
    with socket.socket() as closed:
        closed.bind(("127.0.0.1", 0))
        dead = f"127.0.0.1:{closed.getsockname()[1]}"

    with FakeServer() as live:
        discovery = FakeDiscovery([dead, live.address])
        discovery_class.return_value = discovery

        live.items[b"a:1"] = b'"v"'

        # When: The cache is flushed by prefix.
        with (
            Arcus(ZK_HOSTS, SERVICE_CODE) as client,
            pytest.raises(ArcusErrorGroup) as exc_info,
        ):
            client.flush("a")

    # Then: The live node is flushed, and the error names the unreachable node.
    assert live.items == {}
    (error,) = exc_info.value.exceptions
    assert isinstance(error, ArcusConnectionError)
    assert error.__notes__ == [f"Cache node: {dead}"]


def test_flush_rejected_by_a_node_is_raised_after_the_others(
    discovery_class: MockType,
) -> None:
    with FakeServer() as first, FakeServer() as second:
        # Given: Two nodes holding items of a prefix, the first rejecting the flush.
        serve(discovery_class, first, second)
        first.errors[b"a"] = b"CLIENT_ERROR unauthorized"
        first.items[b"a:1"] = second.items[b"a:2"] = b'"v"'

        # When: The prefix is flushed.
        with (
            Arcus(ZK_HOSTS, SERVICE_CODE) as client,
            pytest.raises(ArcusErrorGroup) as exc_info,
        ):
            client.flush("a")

    # Then: Only the rejecting node keeps its item.
    assert first.items == {b"a:1": b'"v"'}
    assert second.items == {}
    (error,) = exc_info.value.exceptions
    assert isinstance(error, ArcusClientError)


def test_invalid_input_fails_before_discovery(discovery_class: MockType) -> None:
    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        # Given / When / Then: Invalid input fails without starting anything.
        with pytest.raises(ValueError):
            client.get("a b")
        with pytest.raises(TypeError):
            client.set("k", None)
        with pytest.raises(ValueError):
            client.set("k", "v", exptime=-2)
        with pytest.raises(ValueError):
            client.flush("a b")
        with pytest.raises(ValueError):
            client.flush(delay=-1)

    discovery_class.assert_not_called()


def test_server_error_is_raised_and_next_request_reconnects(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A node that fails one key.
    serve(discovery_class, server)
    server.errors[b"bad"] = b"SERVER_ERROR out of memory"

    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        # When: The failing key is stored, and then another key.
        with pytest.raises(ArcusServerError):
            client.set("bad", "v")
        stored = client.set("good", "v")

    # Then: The error is raised and the next request uses a new connection.
    assert stored is True
    assert server.connections == 2


def test_close_stops_discovery_and_rejects_requests(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client that has run a request.
    discovery = serve(discovery_class, server)
    client = Arcus(ZK_HOSTS, SERVICE_CODE)
    client.set("k", "v")

    # When: The client is closed twice.
    client.close()
    client.close()

    # Then: Discovery is stopped and later requests fail.
    assert discovery.stopped
    with pytest.raises(ArcusError, match="client is closed"):
        client.get("k")
