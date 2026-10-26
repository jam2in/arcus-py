import copy
import math
import threading
import time
from collections.abc import Callable

from arcus._transport.sync.connection import SyncConnection
from arcus.exceptions import ArcusError, ArcusPoolTimeoutError


class SyncConnectionPool:
    """Lend up to ``max_size`` connections to a cache node, one request each."""

    def __init__(
        self,
        address: str,
        *,
        max_size: int,
        pool_timeout: float,
        connect: Callable[[str], SyncConnection],
    ) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be a positive integer")
        if not math.isfinite(pool_timeout) or pool_timeout < 0:
            raise ValueError("pool_timeout must be a non-negative finite number")

        self.address = address
        self._max_size = max_size
        self._pool_timeout = pool_timeout
        self._connect = connect
        self._condition = threading.Condition()
        self._idle: list[SyncConnection] = []
        self._in_use: set[SyncConnection] = set()

        # Idle, in-use and connecting connections; never exceeds max_size.
        self._size = 0
        self._close_reason: ArcusError | None = None

    @property
    def in_use(self) -> int:
        with self._condition:
            return len(self._in_use)

    @property
    def idle(self) -> int:
        with self._condition:
            return len(self._idle)

    def acquire(self) -> SyncConnection:
        """Lend an idle connection, or open a new one while under max_size."""
        deadline = time.monotonic() + self._pool_timeout
        with self._condition:
            while True:
                self._raise_if_closed()
                if self._idle:
                    connection = self._idle.pop()
                    self._in_use.add(connection)
                    return connection
                if self._size < self._max_size:
                    self._size += 1
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ArcusPoolTimeoutError(
                        f"No connection available within {self._pool_timeout}s: "
                        f"{self.address}"
                    )
                self._condition.wait(remaining)
        try:
            connection = self._connect(self.address)
        except BaseException:
            with self._condition:
                self._size -= 1
                self._condition.notify()
            raise

        with self._condition:
            closed = self._close_reason is not None
            if closed:
                self._size -= 1
            else:
                self._in_use.add(connection)
        if closed:
            connection.close()
            self._raise_if_closed()
        return connection

    def release(self, connection: SyncConnection) -> None:
        """Take back a connection; broken ones and those after close are closed."""
        with self._condition:
            self._in_use.remove(connection)
            reusable = not connection.broken and self._close_reason is None
            if reusable:
                self._idle.append(connection)
            else:
                self._size -= 1
            self._condition.notify()
        if not reusable:
            connection.close()

    def close(self, reason: ArcusError, *, abort_in_use: bool) -> None:
        """
        Fail pending and later acquire() calls with ``reason``.

        Each caller gets a copy of ``reason`` that keeps only its type and
        message, so pass a new arcus exception built from a single message,
        without a cause.
        """
        with self._condition:
            if self._close_reason is None:
                self._close_reason = reason
                idle, self._idle = self._idle, []
                self._size -= len(idle)
                self._condition.notify_all()
            else:
                idle = []
            in_use = list(self._in_use) if abort_in_use else []

        for connection in idle:
            connection.close()
        for connection in in_use:
            connection.abort()

    def _raise_if_closed(self) -> None:
        if self._close_reason is not None:
            # Copy so that concurrent callers do not share one traceback.
            raise copy.copy(self._close_reason)
