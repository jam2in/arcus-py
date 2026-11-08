import asyncio
import contextlib
import copy
import functools
from typing import TypeVar

from arcus._cluster.discovery import ArcusDiscovery
from arcus._cluster.routing import Routing
from arcus._config import ClientConfig
from arcus._protocol.request import Exchange
from arcus._transport.aio.connection import AsyncConnection
from arcus._transport.aio.pool import AsyncConnectionPool
from arcus.exceptions import (
    ArcusConnectionError,
    ArcusError,
    ArcusOperationTimeoutError,
)

T = TypeVar("T")


class AsyncCluster:
    """
    Discover cache nodes and run each exchange on the node owning its key.

    The first request binds the cluster to its event loop.
    A child process made by fork must create its own cluster: one used before fork
    stays bound to the parent's event loop and rejects the child's.
    """

    def __init__(self, config: ClientConfig) -> None:
        self._config = config

        # Event loop that owns the state below; see _bind_loop()
        self._loop: asyncio.AbstractEventLoop | None = None

        # Shared by requests until the first routing; aclose() waits for it too.
        self._init_task: asyncio.Task[Routing[AsyncConnectionPool]] | None = None
        self._discovery: ArcusDiscovery | None = None

        # Replaced as a whole on the event loop when the cache list changes.
        self._routing: Routing[AsyncConnectionPool] | None = None
        self._closed = False

    async def execute(self, key: bytes, exchange: Exchange[T]) -> T:
        """Run the exchange on a connection to the node that owns the key."""
        self._raise_if_closed()
        self._bind_loop()
        routing = self._routing
        if routing is None:
            routing = await self._initialize()
        pool = routing.pool_for(key)

        connection = await pool.acquire()
        try:
            return await connection.execute(exchange)
        except (ArcusConnectionError, ArcusOperationTimeoutError) as error:
            if self._closed:
                raise ArcusError("client is closed") from error
            raise
        finally:
            pool.release(connection)

    async def aclose(self) -> None:
        """Reject new requests, abort running ones and stop discovery."""
        if self._closed:
            return
        self._bind_loop()
        self._closed = True
        discovery, routing = self._discovery, self._routing
        self._discovery = None
        self._routing = None

        if routing is not None:
            reason = ArcusError("client is closed")
            for pool in routing.pools.values():
                pool.close(reason, abort_in_use=True)

        init_task = self._init_task
        if init_task is not None:
            # The task stops its own discovery once it sees the cluster
            # closed. Wait for it, so that nothing runs after aclose().
            with contextlib.suppress(Exception):
                await asyncio.shield(init_task)

        if discovery is not None:
            await asyncio.to_thread(discovery.stop)

    async def _initialize(self) -> Routing[AsyncConnectionPool]:
        """Return the routing, starting discovery if no request has yet."""
        task = self._init_task
        if task is None:
            task = asyncio.create_task(self._start_discovery())
            self._init_task = task

        try:
            # Shielded, so that cancelling one request leaves the others waiting.
            return await asyncio.shield(task)
        except Exception as error:
            # Copy so that concurrent requests do not share one traceback.
            raise copy.copy(error) from error

    async def _start_discovery(self) -> Routing[AsyncConnectionPool]:
        """Start discovery and return the routing of its initial node list."""
        try:
            discovery = ArcusDiscovery(
                zk_hosts=self._config.zk_hosts,
                service_code=self._config.service_code,
                timeout=self._config.zk_timeout,
            )
            on_change = functools.partial(
                self._on_nodes_changed, asyncio.get_running_loop()
            )
            # start() blocks on ZooKeeper, so it runs in a worker thread.
            # It cleans up its own ZooKeeper client when it fails.
            await asyncio.to_thread(discovery.start, on_change)
            # The watch queued the first list on the loop before start()
            # returned, but the await above does not yield if start() had
            # already finished. Yield once, so that the list is applied.
            await asyncio.sleep(0)

            if self._closed:
                # aclose() ran meanwhile and did not see this discovery.
                await asyncio.to_thread(discovery.stop)
                raise ArcusError("client is closed")

            routing = self._routing
            if routing is None:
                await asyncio.to_thread(discovery.stop)
                raise ArcusConnectionError(
                    f"Failed to load the cache list: {discovery.path}"
                )
            self._discovery = discovery
            return routing
        finally:
            self._init_task = None

    def _on_nodes_changed(
        self, loop: asyncio.AbstractEventLoop, nodes: list[str]
    ) -> None:
        """
        Pass a node list from the ZooKeeper watch to the event loop.

        Only the ZooKeeper watch calls this, one call at a time,
        so the lists reach the loop in order.
        """
        # The loop is closed if the application stopped it without aclose().
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(self._apply_nodes, nodes)

    def _apply_nodes(self, nodes: list[str]) -> None:
        """Publish routing for the nodes and close the pools of removed nodes."""
        if self._closed:
            return

        current = self._routing or Routing()
        routing = current.with_nodes(nodes, self._create_pool)
        self._routing = routing

        for pool in current.pools_missing_from(routing):
            # A node that left the cache list may not answer, so requests
            # still waiting on it fail at once instead of at their timeout.
            pool.close(
                ArcusConnectionError(f"Cache node removed: {pool.address}"),
                abort_in_use=True,
            )

    def _create_pool(self, address: str) -> AsyncConnectionPool:
        return AsyncConnectionPool(
            address,
            max_size=self._config.max_connections_per_node,
            pool_timeout=self._config.pool_timeout,
            connect=self._open_connection,
        )

    async def _open_connection(self, address: str) -> AsyncConnection:
        return await AsyncConnection.open(
            address,
            connection_timeout=self._config.connect_timeout,
            socket_timeout=self._config.socket_timeout,
        )

    def _bind_loop(self) -> None:
        """Bind to the running loop, or reject a loop other than the bound one."""
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise RuntimeError("client is bound to a different event loop")

    def _raise_if_closed(self) -> None:
        """Raise if aclose() was called."""
        if self._closed:
            raise ArcusError("client is closed")
