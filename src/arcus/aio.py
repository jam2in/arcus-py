from types import TracebackType
from typing import Any, Final, Self

from arcus._command.kv import encode_key, prepare_get, prepare_set
from arcus._config import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_MAX_CONNECTIONS_PER_NODE,
    DEFAULT_POOL_TIMEOUT,
    DEFAULT_SOCKET_TIMEOUT,
    DEFAULT_ZK_TIMEOUT,
    ClientConfig,
)
from arcus._transport.aio.cluster import AsyncCluster
from arcus.transcoder import ArcusTranscoder

_TRANSCODER: Final = ArcusTranscoder()


class AsyncArcus:
    """
    A non-blocking Arcus client for one asyncio event loop.

    Nothing connects until the first command runs, and that command binds
    the client to its event loop. A child process made by fork must create
    its own client and leave the inherited one unclosed.
    """

    def __init__(
        self,
        zk_hosts: str,
        service_code: str,
        *,
        max_connections_per_node: int = DEFAULT_MAX_CONNECTIONS_PER_NODE,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        socket_timeout: float = DEFAULT_SOCKET_TIMEOUT,
        pool_timeout: float = DEFAULT_POOL_TIMEOUT,
        zk_timeout: float = DEFAULT_ZK_TIMEOUT,
    ) -> None:
        config = ClientConfig(
            zk_hosts=zk_hosts,
            service_code=service_code,
            max_connections_per_node=max_connections_per_node,
            connect_timeout=connect_timeout,
            socket_timeout=socket_timeout,
            pool_timeout=pool_timeout,
            zk_timeout=zk_timeout,
        )
        self._cluster = AsyncCluster(config)

    async def get(self, key: str) -> Any | None:
        """Return the cached value, or None on a cache miss."""
        key_bytes = encode_key(key)
        exchange = prepare_get(key_bytes, _TRANSCODER)
        return await self._cluster.execute(key_bytes, exchange)

    async def set(self, key: str, value: Any, *, exptime: int = 0) -> bool:
        """Store the value in the cache and return whether it was stored."""
        key_bytes = encode_key(key)
        exchange = prepare_set(key_bytes, value, exptime, _TRANSCODER)
        return await self._cluster.execute(key_bytes, exchange)

    async def aclose(self) -> None:
        await self._cluster.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exe: BaseException | None,
        exc: TracebackType | None,
    ) -> None:
        await self.aclose()
