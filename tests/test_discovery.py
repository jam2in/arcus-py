import pytest
from kazoo.exceptions import ConnectionLoss, NoNodeError, OperationTimeoutError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import EventType, KazooState, WatchedEvent
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


@pytest.fixture
def zk_client(mocker: MockerFixture) -> MockType:
    client_class = mocker.patch(
        "arcus._cluster.discovery.KazooClient",
        autospec=True,
    )
    client = client_class.return_value
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
    zk_client: MockType,
    on_change: MockType,
) -> None:
    # Given: ZooKeeper provides the initial cache node list.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    try:
        # When: Discovery starts.
        addresses = discovery.start(on_change)

        # Then: It returns sorted addresses and registers a watch.
        assert addresses == EXPECTED_ADDRESSES
        zk_client.get_children.assert_called_once_with(
            path=SERVICE_PATH,
            watch=discovery._watch_children,
        )
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
        watch = zk_client.get_children.call_args.kwargs["watch"]
        zk_client.get_children.return_value = children
        event = WatchedEvent(
            EventType.CHILD,
            KazooState.CONNECTED,
            SERVICE_PATH,
        )

        # When: ZooKeeper delivers a child change event.
        watch(event)

        # Then: It notifies the consumer and registers the watch again.
        on_change.assert_called_once_with(expected)
        assert zk_client.get_children.call_count == 2
        zk_client.get_children.assert_called_with(
            path=SERVICE_PATH,
            watch=watch,
        )
    finally:
        discovery.stop()


def test_watch_lookup_failure_does_not_notify(
    zk_client: MockType,
    on_change: MockType,
) -> None:
    # Given: Discovery is started, but the next lookup will fail.
    discovery = ArcusDiscovery(ZK_HOSTS, SERVICE_CODE)

    try:
        discovery.start(on_change)
        watch = zk_client.get_children.call_args.kwargs["watch"]
        zk_client.get_children.side_effect = ConnectionLoss()
        event = WatchedEvent(
            EventType.CHILD,
            KazooState.CONNECTED,
            SERVICE_PATH,
        )

        # When: ZooKeeper delivers a child change event.
        watch(event)

        # Then: The failed lookup does not notify the consumer.
        assert zk_client.get_children.call_count == 2
        on_change.assert_not_called()
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
    watch = zk_client.get_children.call_args.kwargs["watch"]
    event = WatchedEvent(
        EventType.CHILD,
        KazooState.CONNECTED,
        SERVICE_PATH,
    )

    # When: Discovery is stopped and a late event arrives.
    discovery.stop()
    watch(event)

    # Then: Resources are released, and the late event is ignored.
    zk_client.stop.assert_called_once_with()
    zk_client.close.assert_called_once_with()
    assert zk_client.get_children.call_count == 1
    on_change.assert_not_called()
