import asyncio
import copy
import math
from collections.abc import Awaitable, Callable

from arcus._transport.aio.connection import AsyncConnection
from arcus.exceptions import ArcusError, ArcusPoolTimeoutError


class AsyncConnectionPool:
    """Lend up to ``max_size`` connections to a cache node, one request each."""

    def __init__(
        self,
        address: str,
        *,
        max_size: int,
        pool_timeout: float,
        connect: Callable[[str], Awaitable[AsyncConnection]],
    ) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be a positive integer")
        if not math.isfinite(pool_timeout) or pool_timeout < 0:
            raise ValueError("pool_timeout must be a non-negative finite number")

        self.address = address
        self._pool_timeout = pool_timeout
        self._connect = connect
        # One slot per in-use or connecting connection. asyncio.Semaphore
        # queues callers in FIFO order and reserves a released slot for the
        # first waiter, so a caller that has not waited cannot take it.
        self._slots = asyncio.Semaphore(max_size)
        self._idle: list[AsyncConnection] = []
        self._in_use: set[AsyncConnection] = set()
        self._close_reason: ArcusError | None = None

    @property
    def in_use(self) -> int:
        return len(self._in_use)

    @property
    def idle(self) -> int:
        return len(self._idle)

    async def acquire(self) -> AsyncConnection:
        """Lend an idle connection, or open a new one while under max_size."""
        self._raise_if_closed()
        try:
            async with asyncio.timeout(self._pool_timeout):
                await self._slots.acquire()
        except TimeoutError:
            raise ArcusPoolTimeoutError(
                f"No connection available within {self._pool_timeout}s: {self.address}"
            ) from None

        try:
            self._raise_if_closed()
            if self._idle:
                connection = self._idle.pop()
            else:
                connection = await self._connect(self.address)
                if self._close_reason is not None:
                    connection.abort()
                    self._raise_if_closed()
        except BaseException:
            self._slots.release()
            raise

        self._in_use.add(connection)
        return connection

    def release(self, connection: AsyncConnection) -> None:
        """Take back a connection; broken ones and those after close are aborted."""
        self._in_use.remove(connection)
        if connection.broken or self._close_reason is not None:
            connection.abort()
        else:
            self._idle.append(connection)
        self._slots.release()

    def close(self, reason: ArcusError, *, abort_in_use: bool) -> None:
        """
        Fail pending and later acquire() calls with ``reason``.

        Each caller gets a copy of ``reason`` that keeps only its type and
        message, so pass a new arcus exception built from a single message,
        without a cause.
        """
        if self._close_reason is None:
            self._close_reason = reason
            idle, self._idle = self._idle, []
            for connection in idle:
                connection.abort()
            # Wake the first waiter; each one that finds the pool closed
            # releases its slot and so wakes the next.
            self._slots.release()

        if abort_in_use:
            for connection in self._in_use:
                connection.abort()

    def _raise_if_closed(self) -> None:
        if self._close_reason is not None:
            # Copy so that concurrent callers do not share one traceback.
            raise copy.copy(self._close_reason)
