import contextlib
import socket
from typing import TypeVar

from arcus._logging import logger
from arcus._protocol.framing import CRLF, MAX_LINE_LENGTH
from arcus._protocol.request import Exchange, ReadExactly, ReadLine, Write
from arcus.exceptions import ArcusConnectionError, ArcusOperationTimeoutError

T = TypeVar("T")

_RECV_BUFFER_SIZE = 4096


class SyncConnection:
    """A blocking TCP connection used by one request at a time."""

    def __init__(self, address: str, sock: socket.socket) -> None:
        self.address = address
        self.broken = False
        self._socket = sock
        self._buffer = bytearray()

    @classmethod
    def open(
        cls, address: str, *, connection_timeout: float, socket_timeout: float
    ) -> "SyncConnection":
        host, _, port = address.rpartition(":")

        try:
            sock = socket.create_connection(
                (host, int(port)),
                timeout=connection_timeout,
            )
        except OSError as error:
            raise ArcusConnectionError(
                f"Failed to connect to cache node: {address}"
            ) from error

        connection = cls(address, sock)

        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(socket_timeout)
        except OSError as error:
            connection.close()
            raise ArcusConnectionError(
                f"Failed to configure cache connection: {address}"
            ) from error
        except BaseException:
            connection.close()
            raise

        return connection

    def close(self) -> None:
        self.broken = True
        try:
            self._socket.close()
        except OSError:
            logger.exception(f"Failed to close cache connection: {self.address}")

    def abort(self) -> None:
        """Wake a request blocked on this connection from another thread."""
        self.broken = True
        with contextlib.suppress(OSError):
            self._socket.shutdown(socket.SHUT_RDWR)

    def send(self, data: bytes) -> None:
        """Send all bytes without modifying the payload"""
        try:
            self._socket.sendall(data)
        except TimeoutError as error:
            self.close()
            raise ArcusOperationTimeoutError(
                f"Timed out sending to cache node: {self.address}"
            ) from error
        except OSError as error:
            self.close()
            raise ArcusConnectionError(
                f"Failed to send to cache node: {self.address}"
            ) from error
        except BaseException:
            self.close()
            raise

    def recv(self) -> bytes:
        """Receive a chunk of bytes; it may contain only part of a response."""
        try:
            data = self._socket.recv(_RECV_BUFFER_SIZE)
        except TimeoutError as error:
            self.close()
            raise ArcusOperationTimeoutError(
                f"Timed out receiving from cache node: {self.address}"
            ) from error
        except OSError as error:
            self.close()
            raise ArcusConnectionError(
                f"Failed to receive from cache node: {self.address}"
            ) from error
        except BaseException:
            self.close()
            raise

        if not data:
            self.close()
            raise ArcusConnectionError(
                f"Cache node closed the connection: {self.address}"
            )

        return data

    def _read_line(self) -> bytes:
        """Read one CRLF-terminated line without its terminator."""
        while True:
            end = self._buffer.find(CRLF)
            if end >= 0:
                line_length = end
            else:
                line_length = len(self._buffer)
                if self._buffer.endswith(b"\r"):
                    line_length -= 1

            if line_length > MAX_LINE_LENGTH:
                raise ArcusConnectionError(
                    f"Response line exceeds {MAX_LINE_LENGTH} bytes"
                )

            if end >= 0:
                line = bytes(self._buffer[:end])
                del self._buffer[: end + len(CRLF)]
                return line

            self._buffer.extend(self.recv())

    def _read_exactly(self, size: int) -> bytes:
        """Read exactly size bytes, preserving any remaining bytes."""
        if size < 0:
            raise ValueError("Read size must be non-negative")

        while len(self._buffer) < size:
            self._buffer.extend(self.recv())

        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        return data

    def execute(self, exchange: Exchange[T]) -> T:
        """Drive one exchange and return its final result"""
        try:
            try:
                if self.broken:
                    raise ArcusConnectionError(
                        f"Cache connection is not usable: {self.address}"
                    )

                op = next(exchange)
                while True:
                    match op:
                        case Write(data):
                            self.send(data)
                            response = b""
                        case ReadLine():
                            response = self._read_line()
                        case ReadExactly(size):
                            response = self._read_exactly(size)
                    op = exchange.send(response)
            except StopIteration as stop:
                if self._buffer:
                    raise ArcusConnectionError(
                        f"Unexpected data after response: {self.address}"
                    ) from None
                return stop.value
            finally:
                exchange.close()
        except BaseException:
            self.close()
            raise
