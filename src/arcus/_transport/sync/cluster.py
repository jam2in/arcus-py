import copy
import os
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

        # Process that owns the state below; see _reset_after_fork().
        self._pid = os.getpid()
        self._fork_lock = Lock()

        # Lets one thread initialize at a time; close() waits for it too.
        self._init_lock = Lock()
        self._init_error: Exception | None = None
        self._discovery: ArcusDiscovery | None = None

        # Replaced as a whole by the watch; requests read it without a lock.
        self._routing: _Routing | None = None
        self._closed = False

    def execute(self, key: bytes, exchange: Exchange[T]) -> T:
        """Run the exchange on a connection to the node that owns the key."""
        if self._is_forked():
            self._reset_after_fork()
        routing = self._routing
        # Checked after reading routing, so a routing that a late watch
        # published after close() is never used.
        self._raise_if_closed()
        if routing is None:
            routing = self._initialize()
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
        if self._is_forked():
            # The parent still uses these sockets and the ZooKeeper session,
            # so drop them without shutdown or stop.
            self._closed = True
            self._discovery = None
            self._routing = None
            return

        # Waits for an initialization in progress.
        with self._init_lock:
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

    def _initialize(self) -> _Routing:
        """Return the routing, starting discovery if no thread has yet."""
        # Read before waiting for the lock,
        # so a failure while waiting can be told apart.
        error_before_lock = self._init_error
        with self._init_lock:
            self._raise_if_closed()
            if self._routing is not None:
                return self._routing

            failure = self._failure_while_waiting(error_before_lock)
            if failure is not None:
                raise copy.copy(failure) from failure

            try:
                routing = self._start_discovery_locked()
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

    def _start_discovery_locked(self) -> _Routing:
        """Start discovery and return the routing of its initial node list."""
        discovery = ArcusDiscovery(
            zk_hosts=self._config.zk_hosts,
            service_code=self._config.service_code,
            timeout=self._config.zk_timeout,
        )
        # start() cleans up its own ZooKeeper client when it fails.
        discovery.start(self._on_nodes_changed)

        # The watch has passed the first list to _on_nodes_changed by now,
        # unless the list could not be loaded.
        routing = self._routing
        if routing is None:
            discovery.stop()
            raise ArcusConnectionError(
                f"Failed to load the cache list: {discovery.path}"
            )
        self._discovery = discovery
        return routing

    def _on_nodes_changed(self, nodes: list[str]) -> None:
        """
        Publish routing for the nodes and close the pools of removed nodes.

        Only the ZooKeeper watch calls this, one call at a time.
        """
        if self._closed:
            return

        current = self._routing or _Routing()
        routing = current.with_nodes(nodes, self._create_pool)
        self._routing = routing

        for pool in current.pools_missing_from(routing):
            # A node that left the cache list may not answer, so requests
            # still waiting on it fail at once instead of at their timeout.
            pool.close(
                ArcusConnectionError(f"Cache node removed: {pool.address}"),
                abort_in_use=True,
            )

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

    def _reset_after_fork(self) -> None:
        """
        Drop the parent's discovery and connections without closing them.

        The parent still uses its ZooKeeper session and sockets, and a lock
        held by a thread lost in fork never unlocks, so nothing here touches
        the old state. The next request initializes the cluster again.
        """
        if not self._fork_lock.acquire(timeout=5):
            raise ArcusError("Timed out resetting the client after fork")
        try:
            if not self._is_forked():
                return
            self._init_lock = Lock()
            self._discovery = None
            self._routing = None
            self._init_error = None
            # Last, so that other threads still wait for the lock until now.
            self._pid = os.getpid()
        finally:
            self._fork_lock.release()

    def _is_forked(self) -> bool:
        """Read without the lock: a thread lost in fork may still hold it."""
        return self._pid != os.getpid()

    def _raise_if_closed(self) -> None:
        """Raise if close() was called."""
        if self._closed:
            raise ArcusError("client is closed")
