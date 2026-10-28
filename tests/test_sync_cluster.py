import socket
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture, MockType
from support.fake_discovery import FakeDiscovery

from arcus import ArcusConnectionError, ArcusError
from arcus._cluster.locator import ArcusLocator
from arcus._config import ClientConfig
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._transport.sync.cluster import SyncCluster
from arcus._transport.sync.connection import SyncConnection

NODE_A = "127.0.0.1:11211"
NODE_B = "127.0.0.1:11212"
CONFIG = ClientConfig("127.0.0.1:2181", "test")
# Long enough for a waiting thread to block before the main thread acts.
SETTLE = 0.05

MakeCluster = Callable[..., tuple[SyncCluster, MockType]]


def ping() -> Exchange[bytes]:
    yield Write(b"ping\r\n")
    line = yield ReadLine()
    return line


@pytest.fixture
def opened() -> list[SyncConnection]:
    return []


@pytest.fixture
def connect(mocker: MockerFixture, opened: list[SyncConnection]) -> MockType:
    def open_connection(address: str, **_: float) -> SyncConnection:
        sock = mocker.Mock(spec=socket.socket)
        sock.recv.return_value = b"PONG\r\n"
        connection = SyncConnection(address, sock)
        opened.append(connection)
        return connection

    return mocker.patch.object(SyncConnection, "open", side_effect=open_connection)


@pytest.fixture
def make_cluster(
    mocker: MockerFixture,
    connect: MockType,
) -> MakeCluster:
    def make(*discoveries: FakeDiscovery) -> tuple[SyncCluster, MockType]:
        create_discovery = mocker.patch(
            "arcus._transport.sync.cluster.ArcusDiscovery",
            side_effect=list(discoveries),
        )
        cluster = SyncCluster(CONFIG)
        return cluster, create_discovery

    return make


def test_creation_starts_nothing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given / When: A cluster is created.
    _, create_discovery = make_cluster(FakeDiscovery([NODE_A]))

    # Then: Neither discovery nor a connection is started.
    create_discovery.assert_not_called()
    connect.assert_not_called()


def test_execute_routes_to_key_owner_and_reuses_connection(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A cluster with two cache nodes.
    cluster, create_discovery = make_cluster(FakeDiscovery([NODE_A, NODE_B]))
    owner = ArcusLocator([NODE_A, NODE_B]).get_node(b"k")

    # When: Two requests for the same key are executed.
    first = cluster.execute(b"k", ping())
    second = cluster.execute(b"k", ping())

    # Then: Discovery starts once and one connection to the owner is reused.
    assert first == second == b"PONG"
    create_discovery.assert_called_once()
    connect.assert_called_once_with(
        owner,
        connection_timeout=CONFIG.connect_timeout,
        socket_timeout=CONFIG.socket_timeout,
    )


def test_empty_node_list_fails_requests_without_reinitializing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A service with no cache nodes.
    cluster, create_discovery = make_cluster(FakeDiscovery([]))

    # When / Then: Every request fails, but initialization succeeded once.
    for _ in range(2):
        with pytest.raises(ArcusConnectionError, match="no cache nodes"):
            cluster.execute(b"k", ping())
    create_discovery.assert_called_once()
    connect.assert_not_called()


def test_change_during_start_is_applied_after_initial_list(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: The node list changes before start() returns the initial list.
    discovery = FakeDiscovery([NODE_A], nodes_during_start=[NODE_B])
    cluster, _ = make_cluster(discovery)

    # When: Initialization finishes and the waiting change is applied.
    cluster._wait_for_routing()
    discovery.join_watch()
    cluster.execute(b"k", ping())

    # Then: The newer list is used.
    assert [call.args for call in connect.call_args_list] == [(NODE_B,)]


def test_concurrent_first_requests_share_initialization(
    make_cluster: MakeCluster,
) -> None:
    # Given: Initialization that blocks until the test lets it proceed.
    discovery = FakeDiscovery([NODE_A], block=True)
    cluster, create_discovery = make_cluster(discovery)

    with ThreadPoolExecutor(2) as executor:
        first = executor.submit(cluster.execute, b"k", ping())
        assert discovery.entered.wait(timeout=1)
        second = executor.submit(cluster.execute, b"k", ping())
        time.sleep(SETTLE)

        # When: Initialization completes.
        discovery.proceed.set()

        # Then: Both requests succeed with a single discovery.
        assert first.result(timeout=1) == b"PONG"
        assert second.result(timeout=1) == b"PONG"
    create_discovery.assert_called_once()


def test_failed_initialization_is_shared_then_retried(
    make_cluster: MakeCluster,
) -> None:
    # Given: A first initialization that fails while another request waits.
    error = ArcusConnectionError("ZooKeeper discovery startup failed")
    failing = FakeDiscovery([NODE_A], error=error, block=True)
    cluster, create_discovery = make_cluster(failing, FakeDiscovery([NODE_A]))

    with ThreadPoolExecutor(2) as executor:
        first = executor.submit(cluster.execute, b"k", ping())
        assert failing.entered.wait(timeout=1)
        second = executor.submit(cluster.execute, b"k", ping())
        time.sleep(SETTLE)

        # When: The initialization fails.
        failing.proceed.set()

        # Then: The initializer gets the error, and the waiter gets a copy of it.
        with pytest.raises(ArcusConnectionError) as first_error:
            first.result(timeout=1)
        with pytest.raises(ArcusConnectionError) as second_error:
            second.result(timeout=1)
    assert first_error.value is error
    assert second_error.value is not error
    assert second_error.value.__cause__ is error

    # And: A later request starts a new discovery and succeeds.
    assert cluster.execute(b"k", ping()) == b"PONG"
    assert create_discovery.call_count == 2


def test_interrupted_initialization_is_retried_by_waiter(
    make_cluster: MakeCluster,
) -> None:
    # Given: A first initialization that is interrupted while another request waits.
    interrupted = FakeDiscovery([NODE_A], error=KeyboardInterrupt(), block=True)
    cluster, create_discovery = make_cluster(interrupted, FakeDiscovery([NODE_A]))

    with ThreadPoolExecutor(2) as executor:
        first = executor.submit(cluster.execute, b"k", ping())
        assert interrupted.entered.wait(timeout=1)
        second = executor.submit(cluster.execute, b"k", ping())
        time.sleep(SETTLE)

        # When: The initialization is interrupted.
        interrupted.proceed.set()

        # Then: Only the initializer sees the interruption; the waiter starts again.
        with pytest.raises(KeyboardInterrupt):
            first.result(timeout=1)
        assert second.result(timeout=1) == b"PONG"
    assert create_discovery.call_count == 2


def test_broken_connection_is_replaced(
    connect: MockType,
    make_cluster: MakeCluster,
    opened: list[SyncConnection],
) -> None:
    # Given: A connection that breaks while reading the response.
    cluster, _ = make_cluster(FakeDiscovery([NODE_A]))
    cluster.execute(b"k", ping())
    sock = opened[0]._socket
    assert isinstance(sock, Mock)
    sock.recv.side_effect = ConnectionResetError()

    # When: A request fails on it, and another request follows.
    with pytest.raises(ArcusConnectionError):
        cluster.execute(b"k", ping())
    result = cluster.execute(b"k", ping())

    # Then: The broken connection is discarded and a new one is opened.
    assert result == b"PONG"
    assert connect.call_count == 2
    assert opened[0].broken


def key_owned_by(node: str, nodes: list[str]) -> bytes:
    locator = ArcusLocator(nodes)
    return next(
        key
        for key in (f"key-{i}".encode() for i in range(1000))
        if locator.get_node(key) == node
    )


def test_added_node_receives_its_keys(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A cluster that started with one node.
    discovery = FakeDiscovery([NODE_A])
    cluster, _ = make_cluster(discovery)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])
    cluster.execute(key, ping())

    # When: A second node is added.
    discovery.change([NODE_A, NODE_B])
    cluster.execute(key, ping())

    # Then: The key moves to the node that now owns it.
    assert [call.args for call in connect.call_args_list] == [(NODE_A,), (NODE_B,)]


def test_removed_node_closes_idle_connections(
    connect: MockType,
    make_cluster: MakeCluster,
    opened: list[SyncConnection],
) -> None:
    # Given: An idle connection to a node.
    discovery = FakeDiscovery([NODE_A, NODE_B])
    cluster, _ = make_cluster(discovery)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])
    cluster.execute(key, ping())

    # When: The node is removed.
    discovery.change([NODE_A])

    # Then: Its idle connection is closed and the key moves to the remaining node.
    assert opened[0].broken
    cluster.execute(key, ping())
    assert [call.args for call in connect.call_args_list] == [(NODE_B,), (NODE_A,)]


def test_readded_node_gets_new_pool(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A node that was used, then removed.
    discovery = FakeDiscovery([NODE_A, NODE_B])
    cluster, _ = make_cluster(discovery)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])
    cluster.execute(key, ping())
    discovery.change([NODE_A])

    # When: The node is added back.
    discovery.change([NODE_A, NODE_B])
    cluster.execute(key, ping())

    # Then: A new connection is opened instead of failing on the closed pool.
    assert [call.args for call in connect.call_args_list] == [(NODE_B,), (NODE_B,)]


def socket_connect(peers: list[socket.socket]) -> Callable[..., SyncConnection]:
    """Open real socket pairs whose peers stay silent unless the test writes."""

    def open_connection(address: str, **_: float) -> SyncConnection:
        client, peer = socket.socketpair()
        client.settimeout(5)
        peers.append(peer)
        return SyncConnection(address, client)

    return open_connection


def test_close_of_unused_cluster_starts_nothing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A cluster that never ran a request.
    cluster, create_discovery = make_cluster(FakeDiscovery([NODE_A]))

    # When: It is closed twice.
    cluster.close()
    cluster.close()

    # Then: Nothing was started.
    create_discovery.assert_not_called()
    connect.assert_not_called()


def test_close_stops_discovery_and_rejects_requests(
    make_cluster: MakeCluster,
    opened: list[SyncConnection],
) -> None:
    # Given: A cluster with an idle connection.
    discovery = FakeDiscovery([NODE_A])
    cluster, create_discovery = make_cluster(discovery)
    cluster.execute(b"k", ping())

    # When: The cluster is closed.
    cluster.close()

    # Then: Discovery stops, the idle connection is closed and requests fail.
    assert discovery.stopped
    assert opened[0].broken
    with pytest.raises(ArcusError, match="client is closed"):
        cluster.execute(b"k", ping())
    create_discovery.assert_called_once()


def test_close_aborts_in_flight_request(mocker: MockerFixture) -> None:
    # Given: A request waiting for a response that never comes.
    peers: list[socket.socket] = []
    discovery = FakeDiscovery([NODE_A, NODE_B])
    mocker.patch(
        "arcus._transport.sync.cluster.ArcusDiscovery",
        return_value=discovery,
    )
    mocker.patch.object(SyncConnection, "open", side_effect=socket_connect(peers))
    cluster = SyncCluster(CONFIG)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])

    try:
        with ThreadPoolExecutor(1) as executor:
            request = executor.submit(cluster.execute, key, ping())
            while not peers:
                time.sleep(0.01)
            assert peers[0].recv(64) == b"ping\r\n"

            # When: The cluster is closed.
            cluster.close()

            # Then: The request fails at once instead of waiting for the timeout.
            with pytest.raises(ArcusError, match="client is closed") as exc_info:
                request.result(timeout=1)
        assert isinstance(exc_info.value.__cause__, ArcusConnectionError)
    finally:
        for peer in peers:
            peer.close()


def test_removing_node_aborts_in_flight_request(mocker: MockerFixture) -> None:
    # Given: A request waiting for a response that never comes.
    peers: list[socket.socket] = []
    discovery = FakeDiscovery([NODE_A, NODE_B])
    mocker.patch(
        "arcus._transport.sync.cluster.ArcusDiscovery",
        return_value=discovery,
    )
    mocker.patch.object(SyncConnection, "open", side_effect=socket_connect(peers))
    cluster = SyncCluster(CONFIG)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])

    try:
        with ThreadPoolExecutor(1) as executor:
            request = executor.submit(cluster.execute, key, ping())
            while not peers:
                time.sleep(0.01)
            assert peers[0].recv(64) == b"ping\r\n"

            # When: The node leaves the cache list.
            discovery.change([NODE_A])

            # Then: The request fails at once instead of waiting for the timeout.
            with pytest.raises(ArcusConnectionError):
                request.result(timeout=1)
        assert cluster._routing is not None
        assert cluster._routing.pools.keys() == {NODE_A}
    finally:
        for peer in peers:
            peer.close()


def test_close_during_initialization_waits_and_stops_discovery(
    make_cluster: MakeCluster,
) -> None:
    # Given: An initialization in progress.
    discovery = FakeDiscovery([], block=True)
    cluster, _ = make_cluster(discovery)

    with ThreadPoolExecutor(2) as executor:
        first = executor.submit(cluster.execute, b"k", ping())
        assert discovery.entered.wait(timeout=1)

        # When: The cluster is closed.
        closing = executor.submit(cluster.close)
        time.sleep(SETTLE)

        # Then: Close waits until the initialization finishes.
        assert not closing.done()
        discovery.proceed.set()
        with pytest.raises(ArcusConnectionError, match="no cache nodes"):
            first.result(timeout=1)
        closing.result(timeout=1)

    # And: It stops the discovery the initialization started.
    assert discovery.stopped
    with pytest.raises(ArcusError, match="client is closed"):
        cluster.execute(b"k", ping())


def test_change_after_close_registers_nothing(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A closed cluster.
    discovery = FakeDiscovery([NODE_A])
    cluster, _ = make_cluster(discovery)
    cluster.execute(b"k", ping())
    cluster.close()

    # When: A late watch callback delivers a change.
    discovery.change([NODE_A, NODE_B])

    # Then: No pool is created again.
    assert cluster._routing is None
    assert [call.args for call in connect.call_args_list] == [(NODE_A,)]
