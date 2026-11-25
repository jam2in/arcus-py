import pytest

from arcus._cluster.discovery import parse_repl_cache_list
from arcus._cluster.shard import CacheList, Shard

NODE_A = "127.0.0.1:11211"
NODE_B = "127.0.0.1:11212"
NODE_C = "127.0.0.1:11213"
NODE_D = "127.0.0.1:11214"


def test_parse_groups_master_and_slaves() -> None:
    # Given: Two groups, the first with two slaves.
    children = [
        f"g0^S^{NODE_B}-host2",
        f"g0^M^{NODE_A}-host1",
        f"g0^S^{NODE_C}-host3",
        f"g1^M^{NODE_D}-host4",
    ]

    # When: The cache list is parsed.
    result = parse_repl_cache_list(children)

    # Then: Each group is a shard, with its slaves in list order.
    assert result == CacheList(
        {
            "g0": Shard("g0", NODE_A, (NODE_B, NODE_C)),
            "g1": Shard("g1", NODE_D),
        }
    )


@pytest.mark.parametrize(
    "nodes",
    [
        pytest.param(
            [("M", NODE_A), ("S", NODE_B), ("S", NODE_C), ("S", NODE_D)],
            id="too-many-nodes",
        ),
        pytest.param([("M", NODE_A), ("S", NODE_A)], id="duplicate-address"),
        pytest.param([("M", NODE_A), ("M", NODE_B)], id="several-masters"),
        pytest.param([("S", NODE_A), ("S", NODE_B)], id="no-master"),
    ],
)
def test_parse_marks_invalid_group(nodes: list[tuple[str, str]]) -> None:
    # Given: A valid group and a group that breaks one rule.
    children = [f"g0^M^{NODE_A}-host"]
    children += [f"g1^{role}^{address}-host" for role, address in nodes]

    # When: The cache list is parsed.
    result = parse_repl_cache_list(children)

    # Then: Only the valid group is a shard; the other is invalid.
    assert result == CacheList(
        {"g0": Shard("g0", NODE_A)},
        frozenset({"g1"}),
    )


@pytest.mark.parametrize(
    "child",
    [
        pytest.param(f"{NODE_B}-host", id="no-group"),
        pytest.param(f"g1^M-{NODE_B}", id="no-address"),
        pytest.param(f"g1^M^{NODE_B}^x-host", id="extra-field"),
        pytest.param("g1^M^127.0.0.1:0-host", id="invalid-port"),
        pytest.param("g1^M^:11212-host", id="no-host"),
    ],
)
def test_parse_skips_malformed_znode(child: str) -> None:
    # Given: A valid znode and a malformed one.
    children = [f"g0^M^{NODE_A}-host", child]

    # When: The cache list is parsed.
    result = parse_repl_cache_list(children)

    # Then: Only the malformed znode is skipped.
    assert result == CacheList({"g0": Shard("g0", NODE_A)})
