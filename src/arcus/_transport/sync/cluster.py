import copy
from dataclasses import dataclass, field
from threading import Condition, Event
from typing import TypeVar

from arcus._cluster.discovery import ArcusDiscovery
from arcus._cluster.locator import ArcusLocator
from arcus._config import ClientConfig
from arcus._logging import logger
from arcus._protocol.request import Exchange
from arcus._transport.sync.connection import SyncConnection
from arcus._transport.sync.pool import SyncConnectionPool
from arcus.exceptions import (
    ArcusConnectionError,
    ArcusError,
    ArcusOperationTimeoutError,
)

T = TypeVar("T")


@dataclass(slots=True)
class _InitAttempt:
    finished: Event = field(default_factory=Event)
    error: BaseException | None = None
    latest_nodes: list[str] | None = None


class SyncCluster:
    """Discover cache nodes and run each exchange on the node owning its key."""

    def __init__(self, config: ClientConfig) -> None:
        self._config = config

        self._condition = Condition()
        self._closed = False

        self._init_attempt: _InitAttempt | None = None
        self._discovery: ArcusDiscovery | None = None

        self._locator = ArcusLocator([])
        self._pools: dict[str, SyncConnectionPool] = {}

    def execute(self, key: bytes, exchange: Exchange[T]) -> T:
        """Run the exchange on a connection to the node that owns the key."""
        self._ensure_initialized()

        with self._condition:
            self._raise_if_closed()
            address = self._locator.get_node(key)
            pool = self._pools[address]

        connection = pool.acquire()
        try:
            return connection.execute(exchange)
        except (ArcusConnectionError, ArcusOperationTimeoutError) as error:
            with self._condition:
                closed = self._closed
            if closed:
                raise ArcusError("client is closed") from error
            raise
        finally:
            pool.release(connection)

    def close(self) -> None:
        """Reject new requests, abort running ones and stop discovery."""
        with self._condition:
            if self._closed:
                return
            self._closed = True
            attempt = self._init_attempt
            discovery = self._discovery
            pools = list(self._pools.values())
            self._discovery = None
            self._locator = ArcusLocator([])
            self._pools = {}
            # Wake requests waiting for initialization.
            self._condition.notify_all()

        reason = ArcusError("client is closed")
        for pool in pools:
            pool.close(reason, abort_in_use=True)

        # A late initialization stops its own discovery once it sees the close.
        timeout = self._config.zk_timeout + 1
        if attempt is not None and not attempt.finished.wait(timeout):
            logger.warning(f"Initialization did not finish within {timeout}s")

        # Outside the lock: a watch callback may be waiting for it.
        if discovery is not None:
            discovery.stop()

    def _open_connection(self, address: str) -> SyncConnection:
        return SyncConnection.open(
            address,
            connection_timeout=self._config.connect_timeout,
            socket_timeout=self._config.socket_timeout,
        )

    def _create_pool(self, address: str) -> SyncConnectionPool:
        return SyncConnectionPool(
            address,
            max_size=self._config.max_connections_per_node,
            pool_timeout=self._config.pool_timeout,
            connect=self._open_connection,
        )

    def _raise_if_closed(self) -> None:
        """Check the cluster state while holding self._condition."""
        if self._closed:
            raise ArcusError("client is closed")

    def _ensure_initialized(self) -> None:
        with self._condition:
            self._raise_if_closed()
            if self._discovery is not None:
                return

            attempt = self._init_attempt
            if attempt is None or attempt.finished.is_set():
                attempt = _InitAttempt()
                self._init_attempt = attempt
                initialize = True
            else:
                initialize = False

        if initialize:
            # The initializing caller gets its own error unchanged.
            self._initialize(attempt)

        with self._condition:
            while not attempt.finished.is_set():
                self._raise_if_closed()
                self._condition.wait()

            self._raise_if_closed()
            error = attempt.error
            if error is None:
                return
            if isinstance(error, Exception):
                raise copy.copy(error) from error
            raise ArcusError("Client initialization was interrupted") from error

    def _initialize(self, attempt: _InitAttempt) -> None:
        discovery = None
        started = False

        try:
            with self._condition:
                self._raise_if_closed()

            discovery = ArcusDiscovery(
                zk_hosts=self._config.zk_hosts,
                service_code=self._config.service_code,
                timeout=self._config.zk_timeout,
            )
            addresses = discovery.start(
                lambda nodes: self._on_nodes_changed(attempt, nodes)
            )
            started = True

            with self._condition:
                self._raise_if_closed()
                if self._init_attempt is not attempt:
                    raise ArcusError("initialization is no longer active")

                if attempt.latest_nodes is not None:
                    addresses = attempt.latest_nodes

                self._replace_nodes_locked(addresses)
                self._discovery = discovery

        except BaseException as error:
            with self._condition:
                attempt.error = error

            if started and discovery is not None:
                discovery.stop()
            raise

        finally:
            with self._condition:
                attempt.finished.set()
                self._condition.notify_all()

    def _on_nodes_changed(
        self,
        attempt: _InitAttempt,
        addresses: list[str],
    ) -> None:
        with self._condition:
            if (
                self._closed
                or self._init_attempt is not attempt
                or attempt.error is not None
            ):
                return

            if self._discovery is None:
                attempt.latest_nodes = list(addresses)
                return

            for pool in self._replace_nodes_locked(addresses):
                # A node that left the cache list may not answer, so requests
                # still waiting on it fail at once instead of at their timeout.
                pool.close(
                    ArcusConnectionError(f"Cache node removed: {pool.address}"),
                    abort_in_use=True,
                )

    def _replace_nodes_locked(
        self,
        addresses: list[str],
    ) -> list[SyncConnectionPool]:
        """Replace routing state while holding self._condition."""
        nodes = sorted(set(addresses))
        pools: dict[str, SyncConnectionPool] = {}

        for address in nodes:
            if address in self._pools:
                pools[address] = self._pools[address]
            else:
                pools[address] = self._create_pool(address)

        removed = [
            pool for address, pool in self._pools.items() if address not in pools
        ]

        self._locator = ArcusLocator(nodes)
        self._pools = pools
        return removed
