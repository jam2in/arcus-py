import socket
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from pytest_mock import MockerFixture, MockType

from arcus import ArcusConnectionError, ArcusOperationTimeoutError
from arcus._transport.sync.connection import SyncConnection

ADDRESS = "127.0.0.1:11211"
CONNECTION_TIMEOUT = 1.0
SOCKET_TIMEOUT = 0.5


@pytest.fixture
def sock(mocker: MockerFixture) -> MockType:
    return mocker.Mock(spec=socket.socket)


@pytest.fixture
def create_connection(mocker: MockerFixture, sock: MockType) -> MockType:
    return mocker.patch(
        "arcus._transport.sync.connection.socket.create_connection",
        autospec=True,
        return_value=sock,
    )


@pytest.fixture
def connection(sock: MockType) -> SyncConnection:
    return SyncConnection(ADDRESS, sock)


def test_open_configures_socket(
    create_connection: MockType,
    sock: MockType,
) -> None:
    # Given: A TCP connection can be established.

    # When: The connection is opened.
    connection = SyncConnection.open(
        ADDRESS,
        connection_timeout=CONNECTION_TIMEOUT,
        socket_timeout=SOCKET_TIMEOUT,
    )

    # Then: The socket is configured and the connection is usable.
    create_connection.assert_called_once_with(
        ("127.0.0.1", 11211),
        timeout=CONNECTION_TIMEOUT,
    )
    sock.setsockopt.assert_called_once_with(
        socket.IPPROTO_TCP,
        socket.TCP_NODELAY,
        1,
    )
    sock.settimeout.assert_called_once_with(SOCKET_TIMEOUT)
    sock.close.assert_not_called()
    assert connection.address == ADDRESS
    assert not connection.broken


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ConnectionRefusedError(), id="connection-refused"),
        pytest.param(TimeoutError(), id="connection-timeout"),
    ],
)
def test_open_failure_raises_connection_error(
    create_connection: MockType,
    error: OSError,
) -> None:
    # Given: Establishing the TCP connection will fail.
    create_connection.side_effect = error

    # When: The connection is opened.
    with pytest.raises(ArcusConnectionError) as exc_info:
        SyncConnection.open(
            ADDRESS,
            connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=SOCKET_TIMEOUT,
        )

    # Then: The connection error preserves the original cause.
    assert exc_info.value.__cause__ is error


@pytest.mark.parametrize("method", ["setsockopt", "settimeout"])
def test_configuration_failure_closes_socket(
    create_connection: MockType,
    sock: MockType,
    method: str,
) -> None:
    # Given: The socket is connected, but configuration will fail.
    error = OSError("Socket configuration failed")
    getattr(sock, method).side_effect = error

    # When: The connection is opened.
    with pytest.raises(ArcusConnectionError) as exc_info:
        SyncConnection.open(
            ADDRESS,
            connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=SOCKET_TIMEOUT,
        )

    # Then: The socket is closed and the original cause is preserved.
    sock.close.assert_called_once_with()
    assert exc_info.value.__cause__ is error


def test_configuration_interruption_closes_socket(
    create_connection: MockType,
    sock: MockType,
) -> None:
    # Given: Configuration will be interrupted after the socket is connected.
    error = KeyboardInterrupt()
    sock.settimeout.side_effect = error

    # When: The connection is opened.
    with pytest.raises(KeyboardInterrupt) as exc_info:
        SyncConnection.open(
            ADDRESS,
            connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=SOCKET_TIMEOUT,
        )

    # Then: The socket is closed and the interruption is propagated unchanged.
    sock.close.assert_called_once_with()
    assert exc_info.value is error


def test_send_preserves_payload(
    connection: SyncConnection,
    sock: MockType,
) -> None:
    # Given: A complete request already contains the protocol delimiters.
    payload = b"set key 0 0 5\r\nvalue\r\n"

    # When: The request is sent.
    connection.send(payload)

    # Then: All bytes are sent without adding or removing delimiters.
    sock.sendall.assert_called_once_with(payload)
    sock.close.assert_not_called()
    assert not connection.broken


def test_recv_returns_partial_response(
    connection: SyncConnection,
    sock: MockType,
) -> None:
    # Given: Only part of a response is available.
    chunk = b"VALUE ke"
    sock.recv.return_value = chunk

    # When: Data is received.
    result = connection.recv()

    # Then: The chunk is returned without waiting for a complete response.
    assert result == chunk
    sock.recv.assert_called_once()
    sock.close.assert_not_called()
    assert not connection.broken


@pytest.mark.parametrize(
    ("error", "expected_error"),
    [
        pytest.param(TimeoutError(), ArcusOperationTimeoutError, id="timeout"),
        pytest.param(ConnectionResetError(), ArcusConnectionError, id="reset"),
    ],
)
def test_send_failure_closes_connection(
    connection: SyncConnection,
    sock: MockType,
    error: OSError,
    expected_error: type[Exception],
) -> None:
    # Given: Sending the request will fail.
    sock.sendall.side_effect = error

    # When: The request is sent.
    with pytest.raises(expected_error) as exc_info:
        connection.send(b"get key\r\n")

    # Then: The connection is broken and the original cause is preserved.
    assert connection.broken
    sock.close.assert_called_once_with()
    assert exc_info.value.__cause__ is error


@pytest.mark.parametrize(
    ("error", "expected_error"),
    [
        pytest.param(TimeoutError(), ArcusOperationTimeoutError, id="timeout"),
        pytest.param(ConnectionResetError(), ArcusConnectionError, id="reset"),
    ],
)
def test_recv_failure_closes_connection(
    connection: SyncConnection,
    sock: MockType,
    error: OSError,
    expected_error: type[Exception],
) -> None:
    # Given: Receiving data will fail.
    sock.recv.side_effect = error

    # When: Data is received.
    with pytest.raises(expected_error) as exc_info:
        connection.recv()

    # Then: The connection is broken and the original cause is preserved.
    assert connection.broken
    sock.close.assert_called_once_with()
    assert exc_info.value.__cause__ is error


def test_recv_eof_closes_connection(
    connection: SyncConnection,
    sock: MockType,
) -> None:
    # Given: The peer has closed the connection.
    sock.recv.return_value = b""

    # When: Data is received.
    with pytest.raises(ArcusConnectionError):
        connection.recv()

    # Then: The local socket is closed and the connection is marked broken.
    assert connection.broken
    sock.close.assert_called_once_with()


@pytest.mark.parametrize("operation", ["send", "recv"])
def test_io_interruption_closes_connection(
    connection: SyncConnection,
    sock: MockType,
    operation: str,
) -> None:
    # Given: Socket I/O will be interrupted.
    error = KeyboardInterrupt()
    if operation == "send":
        sock.sendall.side_effect = error
    else:
        sock.recv.side_effect = error

    # When: The I/O operation is performed.
    with pytest.raises(KeyboardInterrupt) as exc_info:
        if operation == "send":
            connection.send(b"get key\r\n")
        else:
            connection.recv()

    # Then: The connection is broken and the interruption is propagated unchanged.
    assert connection.broken
    sock.close.assert_called_once_with()
    assert exc_info.value is error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(None, id="success"),
        pytest.param(OSError("Socket close failed"), id="close-failure"),
    ],
)
def test_close_marks_connection_broken(
    connection: SyncConnection,
    sock: MockType,
    error: OSError | None,
) -> None:
    # Given: Closing the socket may succeed or fail.
    sock.close.side_effect = error

    # When: The connection is closed.
    connection.close()

    # Then: The connection is marked broken without propagating close errors.
    assert connection.broken
    sock.close.assert_called_once_with()


def test_cleanup_failure_preserves_original_error(
    connection: SyncConnection,
    sock: MockType,
) -> None:
    # Given: Sending will time out, and closing the socket will also fail.
    error = TimeoutError("Send timed out")
    sock.sendall.side_effect = error
    sock.close.side_effect = OSError("Socket close failed")

    # When: The request is sent.
    with pytest.raises(ArcusOperationTimeoutError) as exc_info:
        connection.send(b"get key\r\n")

    # Then: The cleanup failure does not replace the original timeout.
    assert connection.broken
    sock.close.assert_called_once_with()
    assert exc_info.value.__cause__ is error


def test_abort_shuts_down_socket(connection: SyncConnection, sock: MockType) -> None:
    # Given: An open connection.

    # When: The connection is aborted.
    connection.abort()

    # Then: The socket is shut down but left for the owner to close.
    sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
    sock.close.assert_not_called()
    assert connection.broken


def test_abort_ignores_socket_error(connection: SyncConnection, sock: MockType) -> None:
    # Given: A socket that is already disconnected.
    sock.shutdown.side_effect = OSError("not connected")

    # When: The connection is aborted.
    connection.abort()

    # Then: The error is ignored.
    assert connection.broken


def test_abort_wakes_blocked_recv() -> None:
    # Given: A thread blocked receiving from a silent peer.
    client, peer = socket.socketpair()
    client.settimeout(5)
    connection = SyncConnection(ADDRESS, client)

    with peer, ThreadPoolExecutor(1) as executor:
        receiving = executor.submit(connection.recv)
        time.sleep(0.05)

        # When: Another thread aborts the connection.
        connection.abort()

        # Then: The receiver fails at once instead of waiting for the timeout.
        with pytest.raises(ArcusConnectionError):
            receiving.result(timeout=1)
