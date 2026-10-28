"""The synchronous Arcus client."""

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
from arcus._transport.sync.cluster import SyncCluster
from arcus.transcoder import ArcusTranscoder

# The transcoder keeps no state, so every client shares one.
_TRANSCODER: Final = ArcusTranscoder()


class Arcus:
    """
    A blocking Arcus client that can be shared between threads.

    Nothing connects until the first command runs.
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
        self._cluster = SyncCluster(config)

    def get(self, key: str) -> Any | None:
        """Return the cached value, or None on a cache miss."""
        key_bytes = encode_key(key)
        exchange = prepare_get(key_bytes, _TRANSCODER)
        return self._cluster.execute(key_bytes, exchange)

    def set(self, key: str, value: Any, exptime: int = 0) -> bool:
        """Store the value in the cache and return whether it was stored."""
        key_bytes = encode_key(key)
        exchange = prepare_set(key_bytes, value, exptime, _TRANSCODER)
        return self._cluster.execute(key_bytes, exchange)

    def close(self) -> None:
        """Abort running requests and release every resource; safe to repeat."""
        self._cluster.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
