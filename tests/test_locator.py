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


def test_added_node_takes_its_keys(
    locator: ArcusLocator,
    mocker: MockerFixture,
) -> None:
    # Given: A key between node B's point 100 and node C's point 150.
    mocker.patch("arcus._cluster.locator.key_hash", return_value=120)
    assert locator.get_node(KEY) == NODE_B

    # When: A locator is built with node C added.
    result = ArcusLocator([NODE_A, NODE_B, NODE_C]).get_node(KEY)

    # Then: The key is routed to the new node.
    assert result == NODE_C


@pytest.mark.parametrize("point", [100, 300])
def test_removed_node_leaves_its_points(
    locator: ArcusLocator,
    mocker: MockerFixture,
    point: int,
) -> None:
    # Given: Node A shares point 100 with node B and exclusively owns point 300.
    mocker.patch("arcus._cluster.locator.key_hash", return_value=point)
    assert locator.get_node(KEY) == NODE_A

    # When: A locator is built without node A.
    result = ArcusLocator([NODE_B, NODE_C]).get_node(KEY)

    # Then: Node B keeps point 100, and the removed point 300 wraps to it.
    assert result == NODE_B


def test_empty_locator_raises() -> None:
    # Given: A locator with no nodes.
    locator = ArcusLocator([])

    # When: A key is routed.
    # Then: The absence of cache nodes is reported.
    with pytest.raises(ArcusConnectionError, match="no cache nodes available"):
        locator.get_node(KEY)
