import copy
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from typing import TypeVar

from arcus._cluster.discovery import ArcusDiscovery
from arcus._cluster.locator import ArcusLocator
from arcus._config import ClientConfig
from arcus._protocol.request import Exchange
from arcus._transport.sync.connection import SyncConnection
from arcus._transport.sync.pool import SyncConnectionPool
from arcus.exceptions import (
    ArcusConnectionError,
    ArcusError,
    ArcusOperationTimeoutError,
)

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class _Routing:
    """Which pool serves each key; replaced as a whole when the nodes change."""

    locator: ArcusLocator = field(default_factory=lambda: ArcusLocator([]))
    pools: dict[str, SyncConnectionPool] = field(default_factory=dict)

    def with_nodes(
        self,
        nodes: list[str],
        create_pool: Callable[[str], SyncConnectionPool],
    ) -> "_Routing":
        """Return the routing for the nodes; a node that stays keeps its pool."""
        pools: dict[str, SyncConnectionPool] = {}
        for address in sorted(set(nodes)):
            pools[address] = self.pools.get(address) or create_pool(address)
        return _Routing(ArcusLocator(list(pools)), pools)

    def pools_missing_from(self, other: "_Routing") -> list[SyncConnectionPool]:
        """Return the pools of nodes that other no longer has."""
        return [
            pool for address, pool in self.pools.items() if address not in other.pools
        ]

    def pool_for(self, key: bytes) -> SyncConnectionPool:
        return self.pools[self.locator.get_node(key)]


class SyncCluster:
    """Discover cache nodes and run each exchange on the node owning its key."""

    def __init__(self, config: ClientConfig) -> None:
        self._config = config
        self._lock = Lock()
        self._closed = False
        self._discovery: ArcusDiscovery | None = None
        self._routing: _Routing | None = None
        self._init_error: Exception | None = None

    def execute(self, key: bytes, exchange: Exchange[T]) -> T:
        """Run the exchange on a connection to the node that owns the key."""
        routing = self._routing
        if routing is None:
            routing = self._wait_for_routing()
        pool = routing.pool_for(key)

        connection = pool.acquire()
        try:
            return connection.execute(exchange)
        except (ArcusConnectionError, ArcusOperationTimeoutError) as error:
            if self._closed:
                raise ArcusError("client is closed") from error
            raise
        finally:
            pool.release(connection)

    def close(self) -> None:
        """Reject new requests, abort running ones and stop discovery."""
        # Waits for an initialization in progress, which holds the lock.
        with self._lock:
            if self._closed:
                return
            self._closed = True
            discovery, routing = self._discovery, self._routing
            self._discovery = None
            self._routing = None

        if routing is not None:
            reason = ArcusError("client is closed")
            for pool in routing.pools.values():
                pool.close(reason, abort_in_use=True)

        if discovery is not None:
            discovery.stop()

    def _wait_for_routing(self) -> _Routing:
        # Read before waiting for the lock,
        # so a failure while waiting can be told apart.
        error_before_lock = self._init_error
        with self._lock:
            self._raise_if_closed()
            if self._routing is not None:
                return self._routing

            failure = self._failure_while_waiting(error_before_lock)
            if failure is not None:
                raise copy.copy(failure) from failure

            try:
                routing = self._initialize_locked()
            except Exception as caught:
                self._init_error = caught
                raise
            else:
                self._init_error = None
                return routing

    def _failure_while_waiting(
        self, error_before_lock: Exception | None
    ) -> Exception | None:
        """Return a failure recorded after error_before_lock, while holding lock."""
        error = self._init_error
        return error if error is not error_before_lock else None

    def _initialize_locked(self) -> _Routing:
        """Start discovery and publish its nodes while holding lock."""
        discovery = ArcusDiscovery(
            zk_hosts=self._config.zk_hosts,
            service_code=self._config.service_code,
            timeout=self._config.zk_timeout,
        )
        # start() cleans up its own ZooKeeper client when it fails. A watch
        # that fires meanwhile waits for the lock, so it applies after this.
        nodes = discovery.start(self._on_nodes_changed)
        routing = _Routing().with_nodes(nodes, self._create_pool)

        self._discovery = discovery
        self._routing = routing
        return routing

    def _on_nodes_changed(self, nodes: list[str]) -> None:
        with self._lock:
            # Ignore a late watch after close.
            if self._closed:
                return
            removed = self._replace_nodes_locked(nodes)

        for pool in removed:
            # A node that left the cache list may not answer, so requests
            # still waiting on it fail at once instead of at their timeout.
            pool.close(
                ArcusConnectionError(f"Cache node removed: {pool.address}"),
                abort_in_use=True,
            )

    def _replace_nodes_locked(self, nodes: list[str]) -> list[SyncConnectionPool]:
        """Publish new routing while holding lock; return removed pools."""
        current = self._routing or _Routing()
        self._routing = current.with_nodes(nodes, self._create_pool)
        return current.pools_missing_from(self._routing)

    def _create_pool(self, address: str) -> SyncConnectionPool:
        return SyncConnectionPool(
            address,
            max_size=self._config.max_connections_per_node,
            pool_timeout=self._config.pool_timeout,
            connect=self._open_connection,
        )

    def _open_connection(self, address: str) -> SyncConnection:
        return SyncConnection.open(
            address,
            connection_timeout=self._config.connect_timeout,
            socket_timeout=self._config.socket_timeout,
        )

    def _raise_if_closed(self) -> None:
        """Check the cluster state while holding lock."""
        if self._closed:
            raise ArcusError("client is closed")
