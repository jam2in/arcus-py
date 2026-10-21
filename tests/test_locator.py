import pytest
from pytest_mock import MockerFixture

from arcus import ArcusConnectionError
from arcus._cluster.locator import ArcusLocator, key_hash, node_points

NODE_A = "127.0.0.1:11211"
NODE_B = "127.0.0.1:11212"
NODE_C = "127.0.0.1:11213"
KEY = b"user:1"


@pytest.fixture
def locator(mocker: MockerFixture) -> ArcusLocator:
    mocker.patch(
        "arcus._cluster.locator.node_points",
        side_effect={
            NODE_A: {100, 300},
            NODE_B: {100, 200},
            NODE_C: {150},
        }.__getitem__,
    )
    return ArcusLocator([NODE_B, NODE_A])


def test_key_hash() -> None:
    # Given: A key with a known Ketama hash.
    key = b"foo"

    # When: The key is hashed.
    result = key_hash(key)

    # Then: The result matches the existing client.
    assert result == 3675831724


def test_node_points() -> None:
    # Given: A node address with known Ketama points.
    address = NODE_A

    # When: The node points are generated.
    points = node_points(address)

    # Then: The node has 160 unique points, including the known values.
    assert len(points) == 160
    assert {2589391586, 1482608462, 2562656683, 1506298073} <= points
    assert {2829846691, 3060853474, 3702467535, 611707771} <= points


@pytest.mark.parametrize(
    ("point", "expected"),
    [
        (50, NODE_A),
        (100, NODE_A),
        (101, NODE_B),
        (200, NODE_B),
        (201, NODE_A),
        (300, NODE_A),
        (301, NODE_A),
    ],
)
def test_get_node(
    locator: ArcusLocator,
    mocker: MockerFixture,
    point: int,
    expected: str,
) -> None:
    # Given: A key hashing to a boundary or a position between points.
    mocker.patch("arcus._cluster.locator.key_hash", return_value=point)

    # When: The key is routed.
    result = locator.get_node(KEY)

    # Then: Ceiling selection, address ordering, and wraparound are respected.
    assert result == expected


def test_update_adds_node(
    locator: ArcusLocator,
    mocker: MockerFixture,
) -> None:
    # Given: A key whose owner will change when node C is added.
    mocker.patch("arcus._cluster.locator.key_hash", return_value=120)

    # When: Node C is added to the existing locator.
    locator.update(to_attach={NODE_C}, to_delete=set())

    # Then: The key is routed to the new node.
    assert locator.get_node(KEY) == NODE_C


@pytest.mark.parametrize("point", [100, 300])
def test_update_removes_only_target_node(
    locator: ArcusLocator,
    mocker: MockerFixture,
    point: int,
) -> None:
    # Given: Node A shares point 100 with node B and exclusively owns point 300.
    locator.update(to_attach={NODE_C}, to_delete=set())
    mocker.patch("arcus._cluster.locator.key_hash", return_value=point)

    # When: Node A is removed.
    locator.update(to_attach=set(), to_delete={NODE_A})

    # Then: Node B keeps point 100, and the removed point 300 wraps to it.
    assert locator.get_node(KEY) == NODE_B


def test_empty_locator_raises() -> None:
    # Given: A locator with no nodes.
    locator = ArcusLocator([])

    # When: A key is routed.
    # Then: The absence of cache nodes is reported.
    with pytest.raises(ArcusConnectionError, match="no cache nodes available"):
        locator.get_node(KEY)


def test_update_removes_all_nodes(locator: ArcusLocator) -> None:
    # Given: A locator containing nodes A and B.

    # When: All nodes are removed.
    locator.update(to_attach=set(), to_delete={NODE_A, NODE_B})

    # Then: No stale routing entries remain.
    with pytest.raises(ArcusConnectionError, match="no cache nodes available"):
        locator.get_node(KEY)


def test_update_removes_node_with_duplicate_hash_points() -> None:
    # Given: A node has a duplicate point among its 160 raw hash points.
    address = "127.0.7.1:12932"
    assert len(node_points(address)) == 159

    locator = ArcusLocator([NODE_A, NODE_B])
    original_ring = {point: nodes.copy() for point, nodes in locator._ring.items()}
    original_points = locator._points.copy()

    locator.update(to_attach={address}, to_delete=set())
    assert any(address in nodes for nodes in locator._ring.values())

    # When: The node with duplicate hash points is removed.
    locator.update(to_attach=set(), to_delete={address})

    # Then: The original ring and sorted point index are restored.
    assert locator._ring == original_ring
    assert locator._points == original_points
