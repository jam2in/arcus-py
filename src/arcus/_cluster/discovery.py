from collections.abc import Callable
from typing import Final

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NoNodeError, OperationTimeoutError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.recipe.watchers import ChildrenWatch

from arcus._cluster.shard import CacheList, Shard
from arcus._logging import logger
from arcus.exceptions import ArcusConnectionError

# A master and at most two slaves.
_MAX_GROUP_SIZE: Final = 3


def _is_valid_address(address: str) -> bool:
    host, _, port = address.rpartition(":")
    return (
        bool(host) and port.isascii() and port.isdecimal() and 1 <= int(port) <= 65535
    )


def parse_cache_list(children: list[str]) -> CacheList:
    """Turn ``<ip:port>-<hostname>`` znodes into shards named by address."""
    shards: dict[str, Shard] = {}

    for child in children:
        address = child.partition("-")[0]
        if not _is_valid_address(address):
            logger.warning(f"Ignoring invalid cache node {child}")
            continue
        shards[address] = Shard(address, address)

    return CacheList(shards)


def parse_repl_cache_list(children: list[str]) -> CacheList:
    """
    Group ``<group>^<M|S>^<ip:port>-<hostname>`` znodes into shards.

    A malformed znode is skipped. A group that fails validation goes to
    ``invalid`` instead of ``shards``.
    """
    groups: dict[str, list[tuple[str, str]]] = {}

    for child in children:
        fields = child.partition("-")[0].split("^")
        if len(fields) != 3 or not _is_valid_address(fields[2]):
            logger.warning(f"Ignoring invalid cache node {child}")
            continue

        group, role, address = fields
        groups.setdefault(group, []).append((role, address))

    shards: dict[str, Shard] = {}
    invalid: set[str] = set()
    for group, nodes in groups.items():
        shard = _make_shard(group, nodes)
        if shard is None:
            invalid.add(group)
        else:
            shards[group] = shard

    return CacheList(shards, frozenset(invalid))


def _make_shard(group: str, nodes: list[tuple[str, str]]) -> Shard | None:
    """Return the group's shard, or None if the group is not valid."""
    addresses = [address for _, address in nodes]
    masters = [address for role, address in nodes if role == "M"]

    if len(nodes) > _MAX_GROUP_SIZE:
        logger.warning(f"Ignoring group {group} with too many nodes: {nodes}")
        return None
    if len(set(addresses)) != len(addresses):
        logger.warning(f"Ignoring group {group} with a duplicate address: {nodes}")
        return None
    if len(masters) > 1:
        logger.warning(f"Ignoring group {group} with several masters: {nodes}")
        return None
    if not masters:
        # Normal for a moment during a switchover or failover.
        logger.info(f"Ignoring group {group} without a master: {nodes}")
        return None

    slaves = tuple(address for role, address in nodes if role == "S")
    return Shard(group, masters[0], slaves)


class ArcusDiscovery:
    def __init__(
        self,
        zk_hosts: str,
        service_code: str,
        timeout: float = 10.0,
    ) -> None:
        self._path: str = f"/arcus/cache_list/{service_code}"
        self._client: KazooClient = KazooClient(hosts=zk_hosts)
        self._timeout: float = timeout
        self._closed: bool = False

        # Assigned in start() before registering the watch.
        self._on_change: Callable[[CacheList], None]

    def start(self, on_change: Callable[[CacheList], None]) -> None:
        """
        Connect to ZooKeeper and pass each cache list to on_change.

        The first list is passed in this thread before start() returns,
        unless the service path is deleted meanwhile. Later lists come
        from a kazoo thread, one call at a time.
        """
        self._on_change = on_change

        try:
            self._client.start(timeout=self._timeout)
            if self._client.exists(self._path) is None:
                raise NoNodeError
            ChildrenWatch(self._client, path=self._path, func=self._handle_children)
        except BaseException as error:
            self._closed = True
            self._cleanup()

            if isinstance(error, (KazooTimeoutError, OperationTimeoutError)):
                raise ArcusConnectionError(
                    "ZooKeeper discovery startup timed out"
                ) from error

            if isinstance(error, NoNodeError):
                raise ArcusConnectionError(
                    f"service path not found: {self._path}"
                ) from error

            if isinstance(error, (KazooException, OSError)):
                raise ArcusConnectionError(
                    "ZooKeeper discovery startup failed"
                ) from error

            raise

    def stop(self) -> None:
        self._closed = True
        self._cleanup()

    def _handle_children(self, children: list[str]) -> bool:
        if self._closed:
            return False

        try:
            self._on_change(parse_cache_list(children))
        except Exception:
            logger.exception("Failed to update cache nodes")

        return True

    def _cleanup(self) -> None:
        try:
            self._client.stop()
        except Exception:
            logger.exception("Failed to stop ZooKeeper")

        try:
            self._client.close()
        except Exception:
            logger.exception("Failed to close ZooKeeper")
