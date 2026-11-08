import asyncio
import contextlib
from collections.abc import Awaitable
from typing import TypeVar

from arcus._protocol.framing import CRLF
from arcus._protocol.request import Exchange, Op, ReadExactly, ReadLine, Write
from arcus.exceptions import ArcusConnectionError, ArcusOperationTimeoutError

T = TypeVar("T")


class AsyncConnection:
    """A non-blocking TCP connection used by one request at a time."""

    def __init__(
        self,
        address: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        socket_timeout: float,
    ) -> None:
        self.address = address
        self.broken = False
        self._reader = reader
        self._writer = writer
        self._socket_timeout = socket_timeout

    @classmethod
    async def open(
        cls,
        address: str,
        *,
        connection_timeout: float,
        socket_timeout: float,
    ) -> "AsyncConnection":
        host, _, port = address.rpartition(":")

        try:
            async with asyncio.timeout(connection_timeout):
                reader, writer = await asyncio.open_connection(host, int(port))
        except OSError as error:
            raise ArcusConnectionError(
                f"Failed to connect to cache node: {address}"
            ) from error

        return cls(address, reader, writer, socket_timeout=socket_timeout)

    async def close(self) -> None:
        self.broken = True
        self._writer.close()
        with contextlib.suppress(OSError):
            await self._writer.wait_closed()

    def abort(self) -> None:
        self.broken = True
        self._writer.transport.abort()

    async def send(self, data: bytes) -> None:
        """Send all bytes without modifying the payload."""
        try:
            self._writer.write(data)
            async with asyncio.timeout(self._socket_timeout):
                await self._writer.drain()
        except TimeoutError as error:
            self.abort()
            raise ArcusOperationTimeoutError(
                f"Timed out sending to cache node: {self.address}"
            ) from error
        except OSError as error:
            self.abort()
            raise ArcusConnectionError(
                f"Failed to send to cache node: {self.address}"
            ) from error
        except BaseException:
            self.abort()
            raise

    async def execute(self, exchange: Exchange[T]) -> T:
        """
        Send one request and read its response.

        Any failure or cancellation aborts the connection,
        because the response may be only partly read and the next request
        would read its leftover bytes.
        """
        try:
            self._raise_if_broken()
            return await self._run(exchange)
        except BaseException:
            self.abort()
            raise
        finally:
            exchange.close()

    async def _run(self, exchange: Exchange[T]) -> T:
        """
        Perform each I/O step the exchange asks for until it returns a result.

        The exchange yields a step, receives that step's result
        through send(), and finishes by raising StopIteration with its return value.
        """
        try:
            op = next(exchange)
            while True:
                response = await self._perform(op)
                op = exchange.send(response)
        except StopIteration as finished:
            return finished.value

    async def _perform(self, op: Op) -> bytes:
        """Send bytes, or read a line or a fixed-size block, for one step."""
        match op:
            case Write(data):
                await self.send(data)
                return b""
            case ReadLine():
                reading = self._reader.readuntil(CRLF)
                line = await self._receive(reading)
                return line[: -len(CRLF)]
            case ReadExactly(size):
                reading = self._reader.readexactly(size)
                return await self._receive(reading)

    async def _receive(self, reading: Awaitable[bytes]) -> bytes:
        """Wait for one read to finish within the socket timeout."""
        try:
            async with asyncio.timeout(self._socket_timeout):
                return await reading
        except TimeoutError as error:
            self.abort()
            raise ArcusOperationTimeoutError(
                f"Timed out receiving from cache node: {self.address}"
            ) from error
        except asyncio.IncompleteReadError as error:
            self.abort()
            raise ArcusConnectionError(
                f"Cache node closed the connection: {self.address}"
            ) from error
        except asyncio.LimitOverrunError as error:
            self.abort()
            raise ArcusConnectionError(
                f"Response line is too long: {self.address}"
            ) from error
        except OSError as error:
            self.abort()
            raise ArcusConnectionError(
                f"Failed to receive from cache node: {self.address}"
            ) from error
        except BaseException:
            self.abort()
            raise

    def _raise_if_broken(self) -> None:
        if self.broken:
            raise ArcusConnectionError(
                f"Cache connection is not usable: {self.address}"
            )
