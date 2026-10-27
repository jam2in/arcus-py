import socket
import threading
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
    def open_connection(address: str) -> SyncConnection:
        sock = mocker.Mock(spec=socket.socket)
        sock.recv.return_value = b"PONG\r\n"
        connection = SyncConnection(address, sock)
        opened.append(connection)
        return connection

    return mocker.Mock(side_effect=open_connection)


@pytest.fixture
def make_cluster(
    mocker: MockerFixture,
    connect: MockType,
) -> MakeCluster:
    def make(*discoveries: FakeDiscovery) -> tuple[SyncCluster, MockType]:
        create_discovery = mocker.Mock(side_effect=list(discoveries))
        cluster = SyncCluster(
            CONFIG,
            create_discovery=create_discovery,
            connect=connect,
        )
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
    create_discovery.assert_called_once_with()
    connect.assert_called_once_with(owner)


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
    create_discovery.assert_called_once_with()
    connect.assert_not_called()


def test_change_during_start_replaces_initial_list(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: The node list changes before start() returns the initial list.
    discovery = FakeDiscovery([NODE_A], nodes_during_start=[NODE_B])
    cluster, _ = make_cluster(discovery)

    # When: A request is executed.
    cluster.execute(b"k", ping())

    # Then: The newer list is used.
    connect.assert_called_once_with(NODE_B)


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
    create_discovery.assert_called_once_with()


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


def test_interrupted_initialization_is_not_copied_to_waiters(
    make_cluster: MakeCluster,
) -> None:
    # Given: A first initialization that is interrupted while another request waits.
    interrupt = KeyboardInterrupt()
    discovery = FakeDiscovery([NODE_A], error=interrupt, block=True)
    cluster, _ = make_cluster(discovery)

    with ThreadPoolExecutor(2) as executor:
        first = executor.submit(cluster.execute, b"k", ping())
        assert discovery.entered.wait(timeout=1)
        second = executor.submit(cluster.execute, b"k", ping())
        time.sleep(SETTLE)

        # When: The initialization is interrupted.
        discovery.proceed.set()

        # Then: Only the initializer sees the interruption itself.
        with pytest.raises(KeyboardInterrupt):
            first.result(timeout=1)
        with pytest.raises(ArcusError, match="interrupted") as second_error:
            second.result(timeout=1)
    assert second_error.value.__cause__ is interrupt


def test_late_change_from_failed_attempt_is_ignored(
    connect: MockType,
    make_cluster: MakeCluster,
) -> None:
    # Given: A failed first attempt followed by a successful one.
    failing = FakeDiscovery([NODE_A], error=ArcusConnectionError("failed"))
    cluster, _ = make_cluster(failing, FakeDiscovery([NODE_A]))
    with pytest.raises(ArcusConnectionError):
        cluster.execute(b"k", ping())
    cluster.execute(b"k", ping())

    # When: The failed attempt's watch delivers a late change.
    failing.change([NODE_B])
    cluster.execute(b"k", ping())

    # Then: Routing still follows the successful attempt.
    assert [call.args for call in connect.call_args_list] == [(NODE_A,)]


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


def test_in_flight_request_on_removed_node_completes(
    make_cluster: MakeCluster,
    opened: list[SyncConnection],
) -> None:
    # Given: A request waiting for a response from a node.
    discovery = FakeDiscovery([NODE_A, NODE_B])
    cluster, _ = make_cluster(discovery)
    key = key_owned_by(NODE_B, [NODE_A, NODE_B])
    cluster.execute(key, ping())

    receiving = threading.Event()
    respond = threading.Event()

    def slow_recv(_size: int) -> bytes:
        receiving.set()
        respond.wait()
        return b"PONG\r\n"

    sock = opened[0]._socket
    assert isinstance(sock, Mock)
    sock.recv.side_effect = slow_recv

    with ThreadPoolExecutor(1) as executor:
        request = executor.submit(cluster.execute, key, ping())
        assert receiving.wait(timeout=1)

        # When: The node is removed while the request is in flight.
        discovery.change([NODE_A])
        respond.set()

        # Then: The request completes and its connection is closed afterwards.
        assert request.result(timeout=1) == b"PONG"
    assert opened[0].broken
    # No removed pool is kept once its last request has finished.
    assert cluster._retired_pools == set()


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
