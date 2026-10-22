import socket

from arcus import ArcusConnectionError, ArcusTimeoutError
from arcus._logging import logger

_RECV_BUFFER_SIZE = 4096


class SyncConnection:
    """A blocking TCP connection used by one request at a time."""

    def __init__(self, address: str, sock: socket.socket) -> None:
        self.address = address
        self.broken = False
        self._socket = sock

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

    def send(self, data: bytes) -> None:
        """Send all bytes without modifying the payload"""
        try:
            self._socket.sendall(data)
        except TimeoutError as error:
            self.close()
            raise ArcusTimeoutError(
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
            raise ArcusTimeoutError(
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
