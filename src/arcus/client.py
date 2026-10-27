"""The synchronous Arcus client."""

from types import TracebackType
from typing import Self, TypeVar

from arcus._command.kv import KVCommands
from arcus._config import (
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_MAX_CONNECTIONS_PER_NODE,
    DEFAULT_POOL_TIMEOUT,
    DEFAULT_SOCKET_TIMEOUT,
    DEFAULT_ZK_TIMEOUT,
    ClientConfig,
)
from arcus._protocol.request import Exchange
from arcus._transport.sync.cluster import SyncCluster
from arcus.transcoder import ArcusTranscoder

T = TypeVar("T")


class Arcus(KVCommands):
    """A blocking Arcus client that can be shared between threads.

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
        self._transcoder = ArcusTranscoder()
        self._cluster = SyncCluster(config)

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

    def _execute(self, key: bytes, exchange: Exchange[T]) -> T:
        return self._cluster.execute(key, exchange)
