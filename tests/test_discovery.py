import threading
from collections.abc import Callable
from typing import Any

import pytest
from kazoo.exceptions import ConnectionLoss, NoNodeError, OperationTimeoutError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import EventType, KazooState, WatchedEvent
from kazoo.retry import KazooRetry
from pytest_mock import MockerFixture, MockType

from arcus import ArcusConnectionError
from arcus._cluster.discovery import ArcusDiscovery

ZK_HOSTS = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
SERVICE_CODE = "test"
SERVICE_PATH = "/arcus/cache_list/test"

EXPECTED_ADDRESSES = [
    "127.0.0.1:11211",
    "127.0.0.1:11212",
    "127.0.0.1:11213",
]

CHILD_EVENT = WatchedEvent(EventType.CHILD, KazooState.CONNECTED, SERVICE_PATH)


def _call(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return func(*args, **kwargs)


def _child_watch(zk_client: MockType) -> Callable[[WatchedEvent], None]:
    return zk_client.get_children.call_args.args[1]


def _state_listener(zk_client: MockType) -> Callable[[str], None]:
    return zk_client.add_listener.call_args.args[0]


@pytest.fixture
def zk_client(mocker: MockerFixture) -> MockType:
    client_class = mocker.patch(
        "arcus._cluster.discovery.KazooClient",
        autospec=True,
    )
    client = client_class.return_value
    # KazooClient sets these in __init__, so autospec does not create them.
    # Run retries and spawned tasks inline to keep the tests deterministic.
    client.handler = mocker.Mock()
    client.handler.lock_object.side_effect = threading.Lock
    client.handler.spawn.side_effect = _call
    client.retry = mocker.Mock(side_effect=_call)
    client.get_children.return_value = [
        "127.0.0.1:11213-cache3",
        "127.0.0.1:11211-cache1",
        "127.0.0.1:11212-cache2",
    ]
    return client


@pytest.fixture
def on_change(mocker: MockerFixture) -> MockType:
    return mocker.Mock()


def test_start_returns_cache_nodes(
    mocker: MockerFixture,
    zk_client: MockType,
    on_change: MockType,
) -> None:
    # Given: ZooKeeper provides the initial cache node list.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    try:
        # When: Discovery starts.
        addresses = discovery.start(on_change)

        # Then: It returns sorted addresses without notifying the consumer.
        assert addresses == EXPECTED_ADDRESSES
        zk_client.get_children.assert_called_once_with(
            SERVICE_PATH,
            mocker.ANY,
        )
        zk_client.add_listener.assert_called_once()
        on_change.assert_not_called()
    finally:
        discovery.stop()


@pytest.mark.parametrize(
    ("children", "expected"),
    [
        pytest.param(
            [
                "127.0.0.1:11214-cache4",
                "127.0.0.1:11211-cache1",
            ],
            ["127.0.0.1:11211", "127.0.0.1:11214"],
            id="nodes-changed",
        ),
        pytest.param([], [], id="all-nodes-removed"),
    ],
)
def test_watch_notifies_changes_and_registers_again(
    zk_client: MockType,
    on_change: MockType,
    children: list[str],
    expected: list[str],
) -> None:
    # Given: Discovery is watching the service and nodes have changed.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    try:
        discovery.start(on_change)
        watch = _child_watch(zk_client)
        zk_client.get_children.return_value = children

        # When: ZooKeeper delivers a child change event.
        watch(CHILD_EVENT)

        # Then: It notifies the consumer and registers the watch again.
        on_change.assert_called_once_with(expected)
        assert zk_client.get_children.call_count == 2
        zk_client.get_children.assert_called_with(SERVICE_PATH, watch)
    finally:
        discovery.stop()


@pytest.mark.parametrize(
    "state",
    [
        pytest.param(KazooState.SUSPENDED, id="connection-loss"),
        pytest.param(KazooState.LOST, id="session-expiry"),
    ],
)
def test_reconnect_reads_nodes_changed_while_disconnected(
    zk_client: MockType,
    on_change: MockType,
    state: str,
) -> None:
    # Given: Discovery is watching the service.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    try:
        discovery.start(on_change)
        watch = _child_watch(zk_client)
        listener = _state_listener(zk_client)

        # When: The connection drops, kazoo fires the watch with a NONE event,
        # the nodes change, and the client connects again.
        listener(state)
        watch(WatchedEvent(EventType.NONE, KazooState.CONNECTED, None))
        zk_client.get_children.return_value = ["127.0.0.1:11214-cache4"]
        listener(KazooState.CONNECTED)

        # Then: It reads the nodes only after reconnecting, notifies the
        # consumer, and registers the watch again.
        assert zk_client.get_children.call_count == 2
        on_change.assert_called_once_with(["127.0.0.1:11214"])

        zk_client.get_children.return_value = ["127.0.0.1:11215-cache5"]
        _child_watch(zk_client)(CHILD_EVENT)
        on_change.assert_called_with(["127.0.0.1:11215"])
    finally:
        discovery.stop()


@pytest.mark.parametrize("error", [ConnectionLoss(), OperationTimeoutError()])
def test_watch_retries_lookup_and_keeps_watching(
    zk_client: MockType,
    on_change: MockType,
    error: Exception,
) -> None:
    # Given: A watch refresh fails a few times. Like Kazoo's default retry,
    # max_tries=None retries without limit; delay=0 only skips the sleep.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    try:
        discovery.start(on_change)
        watch = _child_watch(zk_client)
        zk_client.retry.side_effect = KazooRetry(max_tries=None, delay=0)
        zk_client.get_children.side_effect = [
            error,
            error,
            error,
            ["127.0.0.1:11214-cache4"],
        ]

        # When: ZooKeeper delivers a child change event without a reconnect.
        watch(CHILD_EVENT)

        # Then: The retry applies the new list and registers another watch.
        on_change.assert_called_once_with(["127.0.0.1:11214"])
        assert zk_client.get_children.call_count == 5
        zk_client.get_children.assert_called_with(SERVICE_PATH, watch)

        # And: The registered watch handles the following change as well.
        zk_client.get_children.side_effect = None
        zk_client.get_children.return_value = ["127.0.0.1:11215-cache5"]
        _child_watch(zk_client)(CHILD_EVENT)

        on_change.assert_called_with(["127.0.0.1:11215"])
        assert on_change.call_count == 2
        assert zk_client.get_children.call_count == 6
    finally:
        discovery.stop()


def test_watch_applies_next_change_after_callback_failure(
    zk_client: MockType,
    on_change: MockType,
) -> None:
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    try:
        # Given: Applying the next cache list will fail.
        applied_nodes = discovery.start(on_change)
        watch = _child_watch(zk_client)
        zk_client.get_children.return_value = ["127.0.0.1:11214-cache4"]
        on_change.side_effect = RuntimeError("Failed to apply cache nodes")

        # When: ZooKeeper delivers the change and the consumer raises.
        watch(CHILD_EVENT)

        # Then: The old list remains, but another watch is already registered.
        assert applied_nodes == EXPECTED_ADDRESSES
        on_change.assert_called_once_with(["127.0.0.1:11214"])
        assert zk_client.get_children.call_count == 2
        zk_client.get_children.assert_called_with(SERVICE_PATH, watch)

        def apply_nodes(nodes: list[str]) -> None:
            applied_nodes[:] = nodes

        # When: The consumer recovers and a subsequent change arrives.
        on_change.side_effect = apply_nodes
        zk_client.get_children.return_value = ["127.0.0.1:11215-cache5"]
        _child_watch(zk_client)(CHILD_EVENT)

        # Then: The new list is applied and watching continues.
        assert applied_nodes == ["127.0.0.1:11215"]
        assert on_change.call_count == 2
        assert zk_client.get_children.call_count == 3
        zk_client.get_children.assert_called_with(SERVICE_PATH, watch)
    finally:
        discovery.stop()


@pytest.mark.parametrize(
    ("stage", "error"),
    [
        pytest.param("connect", ConnectionLoss(), id="connection-failure"),
        pytest.param("connect", KazooTimeoutError(), id="connection-timeout"),
        pytest.param("lookup", NoNodeError(), id="service-not-found"),
        pytest.param("lookup", OperationTimeoutError(), id="lookup-timeout"),
    ],
)
def test_start_failure_cleans_up(
    zk_client: MockType,
    on_change: MockType,
    stage: str,
    error: Exception,
) -> None:
    # Given: Connecting to ZooKeeper or reading the node list will fail.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    if stage == "connect":
        zk_client.start.side_effect = error
    else:
        zk_client.get_children.side_effect = error

    # When: Discovery attempts to start.
    with pytest.raises(ArcusConnectionError):
        discovery.start(on_change)

    # Then: Resources are released without notifying the consumer.
    zk_client.stop.assert_called_once_with()
    zk_client.close.assert_called_once_with()
    on_change.assert_not_called()


def test_stop_closes_connection_and_ignores_watch(
    zk_client: MockType,
    on_change: MockType,
) -> None:
    # Given: Discovery is watching the service.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)
    discovery.start(on_change)
    watch = _child_watch(zk_client)

    # When: Discovery is stopped and a late event arrives.
    discovery.stop()
    watch(CHILD_EVENT)

    # Then: Resources are released, and the late event is ignored.
    zk_client.stop.assert_called_once_with()
    zk_client.close.assert_called_once_with()
    on_change.assert_not_called()
