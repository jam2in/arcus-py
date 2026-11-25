from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Shard:
    """
    One place on the hash ring: a master and the slaves that copy it.

    Without replication, each cache node is a shard named by its address,
    with no slaves.
    """

    name: str
    master: str
    # TODO: Use the slaves to find the new master on a SWITCHOVER or REPL_SLAVE reply.
    slaves: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CacheList:
    """
    The shards read from one cache list.

    ``invalid`` names the groups that failed validation. Such a group may be
    in the middle of a switchover, so the routing keeps its previous shard.
    """

    shards: dict[str, Shard]
    invalid: frozenset[str] = frozenset()
