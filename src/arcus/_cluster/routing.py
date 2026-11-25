from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from arcus._cluster.locator import ArcusLocator
from arcus._cluster.shard import CacheList, Shard

P = TypeVar("P")


@dataclass(frozen=True, slots=True)
class Routing(Generic[P]):
    """Which pool serves each key; replaced as a whole when the shards change."""

    locator: ArcusLocator = field(default_factory=lambda: ArcusLocator([]))
    shards: dict[str, Shard] = field(default_factory=dict)
    pools: dict[str, P] = field(default_factory=dict)

    def with_shards(
        self,
        cache_list: CacheList,
        create_pool: Callable[[str], P],
    ) -> "Routing[P]":
        """Return the routing for the cache list; a master that stays keeps its pool."""
        shards = cache_list.shards
        pools: dict[str, P] = {}
        for address in sorted({shard.master for shard in shards.values()}):
            pools[address] = self.pools.get(address) or create_pool(address)
        return Routing(ArcusLocator(list(shards)), shards, pools)

    def pools_missing_from(self, other: "Routing[P]") -> list[P]:
        """Return the pools of nodes that other no longer has."""
        return [
            pool for address, pool in self.pools.items() if address not in other.pools
        ]

    def pool_for(self, key: bytes) -> P:
        shard = self.shards[self.locator.get_node(key)]
        return self.pools[shard.master]
