import asyncio
import socket
import threading
from collections.abc import Awaitable, Callable
from typing import cast

import pytest
from pytest_mock import MockerFixture, MockType
from support.fake_discovery import FakeDiscovery

from arcus import ArcusConnectionError, ArcusError
from arcus._cluster.locator import ArcusLocator
from arcus._config import ClientConfig
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._transport.aio.cluster import AsyncCluster
from arcus._transport.aio.connection import AsyncConnection

NODE_A = "127.0.0.1:11211"
NODE_B = "127.0.0.1:11212"
CONFIG = ClientConfig("127.0.0.1:2181", "test")
# Long enough for a waiting task to block before the test acts.
SETTLE = 0.05

MakeCluster = Callable[..., tuple[AsyncCluster, MockType]]


def ping() -> Exchange[bytes]:
    yield Write(b"ping\r\n")
    line = yield ReadLine()
    return line


@pytest.fixture
def opened() -> list[AsyncConnection]:
    return []


@pytest.fixture
def connect(mocker: MockerFixture, opened: list[AsyncConnection]) -> MockType:
    async def open_connection(address: str, **_: float) -> AsyncConnection:
        reader = mocker.Mock(spec=asyncio.StreamReader)
        reader.readuntil.return_value = b"PONG\r\n"
        writer = mocker.Mock(spec=asyncio.StreamWriter)
        connection = AsyncConnection(address, reader, writer, socket_timeout=5)
        opened.append(connection)
        return connection

    return mocker.patch.object(AsyncConnection, "open", side_effect=open_connection)


@pytest.fixture
def make_cluster(mocker: MockerFixture, connect: MockType) -> MakeCluster:
    def make(*discoveries: FakeDiscovery) -> tuple[AsyncCluster, MockType]:
        create_discovery = mocker.patch(
            "arcus._transport.aio.cluster.ArcusDiscovery",
            side_effect=list(discoveries),
        )
        return AsyncCluster(CONFIG), create_discovery

    return make


async def wait_entered(discovery: FakeDiscovery) -> None:
    """Wait without blocking the loop until discovery.start() is running."""
    assert await asyncio.to_thread(discovery.entered.wait, 1)


def key_owned_by(node: str, nodes: list[str]) -> bytes:
    locator = ArcusLocator(nodes)
    return next(
        key
        for key in (f"key-{i}".encode() for i in range(1000))
        if locator.get_node(key) == node
    )


def test_creation_needs_no_loop_and_starts_nothing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given / When: A cluster is created outside any event loop.
    _, create_discovery = make_cluster(FakeDiscovery([NODE_A]))

    # Then: Neither discovery nor a connection is started.
    create_discovery.assert_not_called()
    connect.assert_not_called()


async def test_execute_routes_to_key_owner_and_reuses_connection(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A cluster with two cache nodes.
    cluster, create_discovery = make_cluster(FakeDiscovery([NODE_A, NODE_B]))
    owner = ArcusLocator([NODE_A, NODE_B]).get_node(b"k")

    # When: Two requests for the same key are executed.
    first = await cluster.execute(b"k", ping())
    second = await cluster.execute(b"k", ping())

    # Then: Discovery starts once and one connection to the owner is reused.
    assert first == second == b"PONG"
    create_discovery.assert_called_once()
    connect.assert_called_once_with(
        owner,
        connection_timeout=CONFIG.connect_timeout,
        socket_timeout=CONFIG.socket_timeout,
    )


async def test_empty_node_list_fails_requests_without_reinitializing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A service with no cache nodes.
    cluster, create_discovery = make_cluster(FakeDiscovery([]))

    # When / Then: Every request fails, but initialization succeeded once.
    for _ in range(2):
        with pytest.raises(ArcusConnectionError, match="no cache nodes"):
            await cluster.execute(b"k", ping())
    create_discovery.assert_called_once()
    connect.assert_not_called()


async def test_missing_cache_list_fails_and_stops_discovery(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: Discovery starts but delivers no cache list.
    discovery = FakeDiscovery(None)
    cluster, _ = make_cluster(discovery, FakeDiscovery([NODE_A]))

    # When / Then: The request fails and the discovery is stopped.
    with pytest.raises(ArcusConnectionError, match="Failed to load the cache list"):
        await cluster.execute(b"k", ping())
    assert discovery.stopped
    connect.assert_not_called()

    # And: The next request initializes again.
    assert await cluster.execute(b"k", ping()) == b"PONG"


async def test_first_list_is_applied_when_start_finishes_before_await(
    mocker: MockerFixture,
    make_cluster: MakeCluster,
) -> None:
    # Given: start() has finished in another thread by the time the cluster
    # awaits it, so the await does not yield to the loop.
    async def run_before_await(func: Callable[..., None], /, *args: object) -> None:
        worker = threading.Thread(target=func, args=args)
        worker.start()
        worker.join()

    mocker.patch(
        "arcus._transport.aio.cluster.asyncio.to_thread",
        side_effect=run_before_await,
    )
    cluster, _ = make_cluster(FakeDiscovery([NODE_A]))

    # When / Then: The first list queued on the loop is still used.
    assert await cluster.execute(b"k", ping()) == b"PONG"


async def test_concurrent_first_requests_share_initialization(
    make_cluster: MakeCluster,
) -> None:
    # Given: Two requests waiting for one blocked initialization.
    discovery = FakeDiscovery([NODE_A], block=True)
    cluster, create_discovery = make_cluster(discovery)
    first = asyncio.create_task(cluster.execute(b"k", ping()))
    second = asyncio.create_task(cluster.execute(b"k", ping()))
    await wait_entered(discovery)

    # When: Initialization completes.
    discovery.proceed.set()

    # Then: Both requests succeed with a single discovery.
    assert await asyncio.wait_for(asyncio.gather(first, second), 1) == [
        b"PONG",
        b"PONG",
    ]
    create_discovery.assert_called_once()


async def test_cancelling_one_first_request_keeps_shared_initialization(
    make_cluster: MakeCluster,
) -> None:
    # Given: Two requests waiting for one blocked initialization.
    discovery = FakeDiscovery([NODE_A], block=True)
    cluster, create_discovery = make_cluster(discovery)
    first = asyncio.create_task(cluster.execute(b"k", ping()))
    second = asyncio.create_task(cluster.execute(b"k", ping()))
    await wait_entered(discovery)

    # When: One request is cancelled, then initialization completes.
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    discovery.proceed.set()

    # Then: The other request still succeeds with the same discovery.
    assert await asyncio.wait_for(second, 1) == b"PONG"
    create_discovery.assert_called_once()


async def test_failed_initialization_is_shared_then_retried(
    make_cluster: MakeCluster,
) -> None:
    # Given: Two requests waiting for an initialization that will fail.
    error = ArcusConnectionError("ZooKeeper discovery startup failed")
    failing = FakeDiscovery([NODE_A], error=error, block=True)
    cluster, create_discovery = make_cluster(failing, FakeDiscovery([NODE_A]))
    first = asyncio.create_task(cluster.execute(b"k", ping()))
    second = asyncio.create_task(cluster.execute(b"k", ping()))
    await wait_entered(failing)

    # When: The initialization fails.
    failing.proceed.set()
    results = await asyncio.wait_for(
        asyncio.gather(first, second, return_exceptions=True), 1
    )

    # Then: Each request gets its own copy of the error.
    for result in results:
        assert isinstance(result, ArcusConnectionError)
        assert result is not error
        assert result.__cause__ is error

    # And: A later request starts a new discovery and succeeds.
    assert await cluster.execute(b"k", ping()) == b"PONG"
    assert create_discovery.call_count == 2


async def test_broken_connection_is_replaced(
    connect: MockType,
    make_cluster: MakeCluster,
    opened: list[AsyncConnection],
) -> None:
    # Given: A connection that breaks while reading the response.
    cluster, _ = make_cluster(FakeDiscovery([NODE_A]))
    await cluster.execute(b"k", ping())
    reader = cast(MockType, opened[0]._reader)
    reader.readuntil.side_effect = ConnectionResetError()

    # When: A request fails on it, and another request follows.
    with pytest.raises(ArcusConnectionError):
        await cluster.execute(b"k", ping())
    result = await cluster.execute(b"k", ping())

    # Then: The broken connection is discarded and a new one is opened.
    assert result == b"PONG"
    assert connect.call_count == 2
    assert opened[0].broken


async def test_change_from_zookeeper_thread_applies_on_loop_thread(
    mocker: MockerFixture,
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A cluster that started with one node.
    discovery = FakeDiscovery([NODE_A])
    cluster, _ = make_cluster(discovery)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])
    await cluster.execute(key, ping())
    applied_on: list[int] = []
    apply_nodes = cluster._apply_nodes

    def record_thread(nodes: list[str]) -> None:
        applied_on.append(threading.get_ident())
        apply_nodes(nodes)

    mocker.patch.object(cluster, "_apply_nodes", side_effect=record_thread)

    # When: A second node is added from another thread, as the ZooKeeper watch does.
    await asyncio.to_thread(discovery.change, [NODE_A, NODE_B])
    await asyncio.sleep(0)
    await cluster.execute(key, ping())

    # Then: The change is applied on the loop thread and the key moves.
    assert applied_on == [threading.get_ident()]
    assert [call.args for call in connect.call_args_list] == [(NODE_A,), (NODE_B,)]


async def test_removed_node_closes_idle_connections(
    connect: MockType,
    make_cluster: MakeCluster,
    opened: list[AsyncConnection],
) -> None:
    # Given: An idle connection to a node.
    discovery = FakeDiscovery([NODE_A, NODE_B])
    cluster, _ = make_cluster(discovery)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])
    await cluster.execute(key, ping())

    # When: The node is removed.
    discovery.change([NODE_A])
    await asyncio.sleep(0)

    # Then: Its idle connection is closed and the key moves to the remaining node.
    assert opened[0].broken
    await cluster.execute(key, ping())
    assert [call.args for call in connect.call_args_list] == [(NODE_B,), (NODE_A,)]


def socket_connect(
    peers: list[socket.socket],
    connected: asyncio.Event,
) -> Callable[..., Awaitable[AsyncConnection]]:
    """Open real socket pairs whose peers stay silent unless the test writes."""

    async def open_connection(address: str, **_: float) -> AsyncConnection:
        client, peer = socket.socketpair()
        peer.setblocking(False)
        peers.append(peer)
        connected.set()
        reader, writer = await asyncio.open_connection(sock=client)
        return AsyncConnection(address, reader, writer, socket_timeout=5)

    return open_connection


async def send_request_that_hangs(
    mocker: MockerFixture,
    discovery: FakeDiscovery,
    peers: list[socket.socket],
) -> tuple[AsyncCluster, asyncio.Task[bytes]]:
    """Start a request to NODE_B that waits for a response that never comes."""
    mocker.patch(
        "arcus._transport.aio.cluster.ArcusDiscovery",
        return_value=discovery,
    )
    connected = asyncio.Event()
    mocker.patch.object(
        AsyncConnection, "open", side_effect=socket_connect(peers, connected)
    )
    cluster = AsyncCluster(CONFIG)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])
    request = asyncio.create_task(cluster.execute(key, ping()))

    await asyncio.wait_for(connected.wait(), 1)
    loop = asyncio.get_running_loop()
    assert await loop.sock_recv(peers[0], 64) == b"ping\r\n"
    return cluster, request


async def test_aclose_aborts_in_flight_request(mocker: MockerFixture) -> None:
    peers: list[socket.socket] = []
    try:
        # Given: A request waiting for a response that never comes.
        cluster, request = await send_request_that_hangs(
            mocker, FakeDiscovery([NODE_A, NODE_B]), peers
        )

        # When: The cluster is closed.
        await cluster.aclose()

        # Then: The request fails at once instead of waiting for the timeout.
        with pytest.raises(ArcusError, match="client is closed") as exc_info:
            await asyncio.wait_for(request, 1)
        assert isinstance(exc_info.value.__cause__, ArcusConnectionError)
    finally:
        for peer in peers:
            peer.close()


async def test_removing_node_aborts_in_flight_request(mocker: MockerFixture) -> None:
    peers: list[socket.socket] = []
    discovery = FakeDiscovery([NODE_A, NODE_B])
    try:
        # Given: A request waiting for a response that never comes.
        cluster, request = await send_request_that_hangs(mocker, discovery, peers)

        # When: The node leaves the cache list.
        discovery.change([NODE_A])

        # Then: The request fails at once instead of waiting for the timeout.
        with pytest.raises(ArcusConnectionError):
            await asyncio.wait_for(request, 1)
        assert cluster._routing is not None
        assert cluster._routing.pools.keys() == {NODE_A}
    finally:
        for peer in peers:
            peer.close()


async def test_aclose_of_unused_cluster_starts_nothing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A cluster that never ran a request.
    cluster, create_discovery = make_cluster(FakeDiscovery([NODE_A]))

    # When: It is closed twice.
    await cluster.aclose()
    await cluster.aclose()

    # Then: Nothing was started.
    create_discovery.assert_not_called()
    connect.assert_not_called()


async def test_aclose_stops_discovery_and_rejects_requests(
    make_cluster: MakeCluster,
    opened: list[AsyncConnection],
) -> None:
    # Given: A cluster with an idle connection.
    discovery = FakeDiscovery([NODE_A])
    cluster, create_discovery = make_cluster(discovery)
    await cluster.execute(b"k", ping())

    # When: The cluster is closed.
    await cluster.aclose()

    # Then: Discovery stops, the idle connection is closed and requests fail.
    assert discovery.stopped
    assert opened[0].broken
    with pytest.raises(ArcusError, match="client is closed"):
        await cluster.execute(b"k", ping())
    create_discovery.assert_called_once()


async def test_aclose_during_initialization_waits_and_stops_discovery(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: An initialization in progress.
    discovery = FakeDiscovery([NODE_A], block=True)
    cluster, _ = make_cluster(discovery)
    request = asyncio.create_task(cluster.execute(b"k", ping()))
    await wait_entered(discovery)

    # When: The cluster is closed.
    closing = asyncio.create_task(cluster.aclose())
    await asyncio.sleep(SETTLE)

    # Then: aclose() waits until the initialization finishes.
    assert not closing.done()
    discovery.proceed.set()
    await asyncio.wait_for(closing, 1)

    # And: It stops the discovery, and the waiting request fails.
    assert discovery.stopped
    with pytest.raises(ArcusError, match="client is closed"):
        await request
    connect.assert_not_called()


async def test_change_after_aclose_registers_nothing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A closed cluster.
    discovery = FakeDiscovery([NODE_A])
    cluster, _ = make_cluster(discovery)
    await cluster.execute(b"k", ping())
    await cluster.aclose()

    # When: A late watch callback delivers a change.
    discovery.change([NODE_A, NODE_B])
    await asyncio.sleep(0)

    # Then: No pool is created again.
    assert cluster._routing is None
    assert [call.args for call in connect.call_args_list] == [(NODE_A,)]


def test_other_event_loop_is_rejected(make_cluster: MakeCluster) -> None:
    # Given: A cluster used on one event loop.
    cluster, _ = make_cluster(FakeDiscovery([NODE_A]))
    asyncio.run(cluster.execute(b"k", ping()))

    # When / Then: Another event loop cannot use or close it.
    with pytest.raises(RuntimeError, match="different event loop"):
        asyncio.run(cluster.execute(b"k", ping()))
    with pytest.raises(RuntimeError, match="different event loop"):
        asyncio.run(cluster.aclose())


def test_change_after_loop_closed_is_ignored(make_cluster: MakeCluster) -> None:
    # Given: A cluster whose event loop has finished without aclose().
    discovery = FakeDiscovery([NODE_A])
    cluster, _ = make_cluster(discovery)
    asyncio.run(cluster.execute(b"k", ping()))

    # When / Then: A later watch callback does not raise.
    discovery.change([NODE_A, NODE_B])
