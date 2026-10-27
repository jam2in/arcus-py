from collections.abc import Iterator

import pytest
from pytest_mock import MockerFixture, MockType
from support.fake_discovery import FakeDiscovery
from support.fake_server import FakeServer

import arcus
from arcus import Arcus, ArcusError, ArcusServerError
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


def test_key_is_stored_on_its_owner_node(discovery_class: MockType) -> None:
    with FakeServer() as first, FakeServer() as second:
        # Given: A service with two cache nodes.
        serve(discovery_class, first, second)
        owner = ArcusLocator([first.address, second.address]).get_node(b"k")

        # When: A value is stored.
        with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
            client.set("k", "v")

    # Then: Only the owner node holds it.
    holders = [s.address for s in (first, second) if b"k" in s.items]
    assert holders == [owner]


def test_invalid_input_fails_before_discovery(discovery_class: MockType) -> None:
    with Arcus(ZK_HOSTS, SERVICE_CODE) as client:
        # Given / When / Then: Invalid input fails without starting anything.
        with pytest.raises(ValueError):
            client.get("a b")
        with pytest.raises(TypeError):
            client.set("k", None)

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
