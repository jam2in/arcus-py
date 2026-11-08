from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from arcus._cluster.locator import ArcusLocator

P = TypeVar("P")


@dataclass(frozen=True, slots=True)
class Routing(Generic[P]):
    """Which pool serves each key; replaced as a whole when the nodes change."""

    locator: ArcusLocator = field(default_factory=lambda: ArcusLocator([]))
    pools: dict[str, P] = field(default_factory=dict)

    def with_nodes(
        self,
        nodes: list[str],
        create_pool: Callable[[str], P],
    ) -> "Routing[P]":
        """Return the routing for the nodes; a node that stays keeps its pool."""
        pools: dict[str, P] = {}
        for address in sorted(set(nodes)):
            pools[address] = self.pools.get(address) or create_pool(address)
        return Routing(ArcusLocator(list(pools)), pools)

    def pools_missing_from(self, other: "Routing[P]") -> list[P]:
        """Return the pools of nodes that other no longer has."""
        return [
            pool for address, pool in self.pools.items() if address not in other.pools
        ]

    def pool_for(self, key: bytes) -> P:
        return self.pools[self.locator.get_node(key)]
