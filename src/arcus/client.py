"""The synchronous Arcus client."""

from collections.abc import Sequence
from types import TracebackType
from typing import Any, Final, Self

from arcus._command import encode_key
from arcus._command.kv import prepare_get, prepare_set
from arcus._command.vector import (
    Metric,
    Quantization,
    SearchResult,
    Vector,
    VopCreateOptions,
    prepare_vop_create,
    prepare_vop_search,
    prepare_vop_upsert,
)
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

    def set(self, key: str, value: Any, *, exptime: int = 0) -> bool:
        """Store the value in the cache and return whether it was stored."""
        key_bytes = encode_key(key)
        exchange = prepare_set(key_bytes, value, exptime, _TRANSCODER)
        return self._cluster.execute(key_bytes, exchange)

    def vop_create(
        self,
        index: str,
        dim: int,
        *,
        metric: str = Metric.COSINE,
        quant: str = Quantization.F32,
        m: int = 16,
        ef_construction: int = 128,
        ef_search: int = 64,
        exptime: int = 0,
        maxcount: int = 0,
    ) -> "Vector[Arcus] | None":
        """Create a vector index and return its handle, or None if the key is taken."""
        index_bytes = encode_key(index)
        options = VopCreateOptions(
            exptime=exptime,
            maxcount=maxcount,
            metric=metric,
            quant=quant,
            m=m,
            ef_construction=ef_construction,
            ef_search=ef_search,
        )
        exchange = prepare_vop_create(index_bytes, dim, options)
        if not self._cluster.execute(index_bytes, exchange):
            return None
        return Vector(self, index)

    def vop_upsert(
        self,
        index: str,
        vkey: str,
        vector: Sequence[float] | bytes,
        *,
        meta: dict[str, Any] | None = None,
    ) -> bool:
        """Store the vector under the key; False if the index is missing or full."""
        index_bytes = encode_key(index)
        exchange = prepare_vop_upsert(index_bytes, vkey, vector, meta)
        return self._cluster.execute(index_bytes, exchange)

    def vop_search(
        self,
        index: str,
        vector: Sequence[float] | bytes | str,
        num: int,
        *,
        filter: str | None = None,
        with_meta: bool = False,
    ) -> list[SearchResult] | None:
        """Return the num nearest elements; None if the index or vkey is missing."""
        index_bytes = encode_key(index)
        exchange = prepare_vop_search(index_bytes, vector, num, filter, with_meta)
        return self._cluster.execute(index_bytes, exchange)

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
