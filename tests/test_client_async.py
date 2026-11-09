import asyncio
from collections.abc import Iterator

import pytest
from pytest_mock import MockerFixture, MockType
from support.fake_discovery import FakeDiscovery
from support.fake_server import FakeServer

import arcus
from arcus import ArcusError, ArcusServerError, AsyncArcus
from arcus._cluster.locator import ArcusLocator

ZK_HOSTS = "127.0.0.1:2181"
SERVICE_CODE = "test"


@pytest.fixture
def server() -> Iterator[FakeServer]:
    with FakeServer() as server:
        yield server


@pytest.fixture
def discovery_class(mocker: MockerFixture) -> MockType:
    return mocker.patch("arcus._transport.aio.cluster.ArcusDiscovery", autospec=True)


def serve(discovery_class: MockType, *servers: FakeServer) -> FakeDiscovery:
    discovery = FakeDiscovery([server.address for server in servers])
    discovery_class.return_value = discovery
    return discovery


def test_async_arcus_is_exported() -> None:
    assert arcus.AsyncArcus is AsyncArcus
    assert "AsyncArcus" in arcus.__all__


def test_invalid_config_is_rejected_on_creation() -> None:
    # Given / When / Then: An empty service code fails before anything starts.
    with pytest.raises(ValueError, match="service_code"):
        AsyncArcus(ZK_HOSTS, "")


def test_creation_needs_no_event_loop(discovery_class: MockType) -> None:
    # Given / When: A client is created outside any event loop.
    AsyncArcus(ZK_HOSTS, SERVICE_CODE)

    # Then: Discovery was never started.
    discovery_class.assert_not_called()


async def test_async_with_starts_nothing(discovery_class: MockType) -> None:
    # Given / When: A client is used as an async context manager.
    async with AsyncArcus(ZK_HOSTS, SERVICE_CODE):
        pass

    # Then: Discovery was never started.
    discovery_class.assert_not_called()


async def test_set_then_get(server: FakeServer, discovery_class: MockType) -> None:
    # Given: A client for a service with one cache node.
    discovery = serve(discovery_class, server)

    async with AsyncArcus(ZK_HOSTS, SERVICE_CODE, zk_timeout=3.0) as client:
        # When: A value is stored and read back.
        stored = await client.set("k", {"a": [1, 2]})
        value = await client.get("k")
        missing = await client.get("other")

    # Then: The value round-trips, discovery got the client settings,
    # and leaving the block stopped it.
    assert stored is True
    assert value == {"a": [1, 2]}
    assert missing is None
    discovery_class.assert_called_once_with(
        zk_hosts=ZK_HOSTS,
        service_code=SERVICE_CODE,
        timeout=3.0,
    )
    assert discovery.stopped
    assert server.connections == 1


async def test_key_is_stored_on_its_owner_node(discovery_class: MockType) -> None:
    key = "가".encode()

    with FakeServer() as first, FakeServer() as second:
        # Given: A service with two cache nodes.
        serve(discovery_class, first, second)
        owner = ArcusLocator([first.address, second.address]).get_node(key)

        # When: A value is stored under a non-ASCII key.
        async with AsyncArcus(ZK_HOSTS, SERVICE_CODE) as client:
            await client.set("가", "v")

    # Then: Only the node that owns the UTF-8 key holds it.
    holders = [s.address for s in (first, second) if key in s.items]
    assert holders == [owner]


async def test_concurrent_requests_share_the_connection_limit(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client that may open two connections to its one node.
    serve(discovery_class, server)

    async with AsyncArcus(ZK_HOSTS, SERVICE_CODE, max_connections_per_node=2) as client:
        # When: Many requests run at the same time.
        results = await asyncio.gather(*(client.set(f"key-{i}", i) for i in range(20)))

    # Then: All succeed over at most two connections.
    assert results == [True] * 20
    assert 1 <= server.connections <= 2


async def test_invalid_input_fails_before_discovery(
    discovery_class: MockType,
) -> None:
    async with AsyncArcus(ZK_HOSTS, SERVICE_CODE) as client:
        # Given / When / Then: Invalid input fails without starting anything.
        with pytest.raises(ValueError):
            await client.get("a b")
        with pytest.raises(TypeError):
            await client.set("k", None)
        with pytest.raises(ValueError):
            await client.set("k", "v", exptime=-2)

    discovery_class.assert_not_called()


async def test_server_error_is_raised_and_next_request_reconnects(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A node that fails one key.
    serve(discovery_class, server)
    server.errors[b"bad"] = b"SERVER_ERROR out of memory"

    async with AsyncArcus(ZK_HOSTS, SERVICE_CODE) as client:
        # When: The failing key is stored, and then another key.
        with pytest.raises(ArcusServerError):
            await client.set("bad", "v")
        stored = await client.set("good", "v")

    # Then: The error is raised and the next request uses a new connection.
    assert stored is True
    assert server.connections == 2


async def test_aclose_stops_discovery_and_rejects_requests(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client that has run a request.
    discovery = serve(discovery_class, server)
    client = AsyncArcus(ZK_HOSTS, SERVICE_CODE)
    await client.set("k", "v")

    # When: The client is closed twice.
    await client.aclose()
    await client.aclose()

    # Then: Discovery is stopped and later requests fail.
    assert discovery.stopped
    with pytest.raises(ArcusError, match="client is closed"):
        await client.get("k")


def test_other_event_loop_is_rejected(
    server: FakeServer,
    discovery_class: MockType,
) -> None:
    # Given: A client used on one event loop.
    serve(discovery_class, server)
    client = AsyncArcus(ZK_HOSTS, SERVICE_CODE)
    asyncio.run(client.set("k", "v"))

    # When / Then: Another event loop cannot use it.
    with pytest.raises(RuntimeError, match="different event loop"):
        asyncio.run(client.get("k"))
