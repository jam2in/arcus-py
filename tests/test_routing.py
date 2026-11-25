from arcus._cluster.locator import ArcusLocator
from arcus._cluster.routing import Routing
from arcus._cluster.shard import CacheList, Shard

NODE_A = "127.0.0.1:11211"
NODE_B = "127.0.0.1:11212"
KEY = b"user:1"


def test_pool_for_hashes_shard_name_and_uses_its_master() -> None:
    # Given: Shards whose names differ from their master addresses.
    shards = {"g0": Shard("g0", NODE_A), "g1": Shard("g1", NODE_B)}

    # When: The routing for those shards picks the pool for a key.
    routing = Routing[str]().with_shards(CacheList(shards), lambda address: address)
    result = routing.pool_for(KEY)

    # Then: The ring hashes shard names, and the owner's master serves the key.
    owner = ArcusLocator(["g0", "g1"]).get_node(KEY)
    assert result == shards[owner].master
