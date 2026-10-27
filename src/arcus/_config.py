"""Validated settings shared by the sync and async clients."""

import math
from dataclasses import dataclass
from typing import Final

DEFAULT_MAX_CONNECTIONS_PER_NODE: Final[int] = 8
DEFAULT_CONNECT_TIMEOUT: Final[float] = 5.0
DEFAULT_SOCKET_TIMEOUT: Final[float] = 5.0
DEFAULT_POOL_TIMEOUT: Final[float] = 5.0
DEFAULT_ZK_TIMEOUT: Final[float] = 10.0


@dataclass(frozen=True, slots=True)
class ClientConfig:
    zk_hosts: str
    service_code: str
    max_connections_per_node: int = DEFAULT_MAX_CONNECTIONS_PER_NODE
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT
    socket_timeout: float = DEFAULT_SOCKET_TIMEOUT
    pool_timeout: float = DEFAULT_POOL_TIMEOUT
    zk_timeout: float = DEFAULT_ZK_TIMEOUT

    def __post_init__(self) -> None:
        for name, value in (
            ("zk_hosts", self.zk_hosts),
            ("service_code", self.service_code),
        ):
            if not isinstance(value, str):
                raise TypeError(f"{name} must be a string")
            if not value.strip():
                raise ValueError(f"{name} must not be empty")

        limit = self.max_connections_per_node
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError("max_connections_per_node must be an integer")
        if limit < 1:
            raise ValueError("max_connections_per_node must be at least 1")

        for name, timeout in (
            ("connect_timeout", self.connect_timeout),
            ("socket_timeout", self.socket_timeout),
            ("pool_timeout", self.pool_timeout),
            ("zk_timeout", self.zk_timeout),
        ):
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise TypeError(f"{name} must be a number")

            try:
                finite = math.isfinite(timeout)
            except OverflowError:
                finite = False

            if not finite or timeout <= 0:
                raise ValueError(f"{name} must be a positive finite number")
