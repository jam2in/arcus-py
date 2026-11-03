import asyncio
import socket
from collections.abc import Callable

import pytest
from pytest_mock import MockerFixture, MockType
from support.response_cases import CASES, Case

from arcus import ArcusConnectionError, ArcusOperationTimeoutError
from arcus._command.kv import prepare_get
from arcus._protocol.request import Exchange, ReadLine
from arcus._transport.aio.connection import AsyncConnection
from arcus.transcoder import ArcusTranscoder

ADDRESS = "127.0.0.1:11211"
CONNECTION_TIMEOUT = 0.05
SOCKET_TIMEOUT = 0.05

T = ArcusTranscoder()


async def stall(*_args: object) -> bytes:
    """Wait longer than any timeout in this module."""
    await asyncio.sleep(10)
    return b""


@pytest.fixture
async def reader() -> asyncio.StreamReader:
    return asyncio.StreamReader()


@pytest.fixture
def writer(mocker: MockerFixture) -> MockType:
    return mocker.Mock(spec=asyncio.StreamWriter)


@pytest.fixture
def open_connection(
    mocker: MockerFixture,
    reader: asyncio.StreamReader,
    writer: MockType,
) -> MockType:
    return mocker.patch(
        "arcus._transport.aio.connection.asyncio.open_connection",
        autospec=True,
        return_value=(reader, writer),
    )


@pytest.fixture
def connection(reader: asyncio.StreamReader, writer: MockType) -> AsyncConnection:
    return AsyncConnection(ADDRESS, reader, writer, socket_timeout=SOCKET_TIMEOUT)


async def open_pair(
    socket_timeout: float = 5,
) -> tuple[AsyncConnection, socket.socket]:
    """Open a connection on a real socket and return it with its peer."""
    client, peer = socket.socketpair()
    peer.setblocking(False)
    reader, writer = await asyncio.open_connection(sock=client)
    connection = AsyncConnection(ADDRESS, reader, writer, socket_timeout=socket_timeout)
    return connection, peer


async def test_open_returns_connection(open_connection: MockType) -> None:
    # Given: A TCP connection can be established.

    # When: The connection is opened.
    connection = await AsyncConnection.open(
        ADDRESS,
        connection_timeout=CONNECTION_TIMEOUT,
        socket_timeout=SOCKET_TIMEOUT,
    )

    # Then: The connection is usable.
    open_connection.assert_awaited_once_with("127.0.0.1", 11211)
    assert connection.address == ADDRESS
    assert not connection.broken


async def test_open_failure_raises_connection_error(
    open_connection: MockType,
) -> None:
    # Given: Establishing the TCP connection will fail.
    error = ConnectionRefusedError()
    open_connection.side_effect = error

    # When: The connection is opened.
    with pytest.raises(ArcusConnectionError) as exc_info:
        await AsyncConnection.open(
            ADDRESS,
            connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=SOCKET_TIMEOUT,
        )

    # Then: The connection error preserves the original cause.
    assert exc_info.value.__cause__ is error


async def test_open_timeout_raises_connection_error(
    open_connection: MockType,
) -> None:
    # Given: Establishing the TCP connection will not finish in time.
    open_connection.side_effect = stall

    # When: The connection is opened.
    with pytest.raises(ArcusConnectionError) as exc_info:
        await AsyncConnection.open(
            ADDRESS,
            connection_timeout=CONNECTION_TIMEOUT,
            socket_timeout=SOCKET_TIMEOUT,
        )

    # Then: The timeout is reported as a connection error, not an operation timeout.
    assert not isinstance(exc_info.value, ArcusOperationTimeoutError)
    assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_open_cancellation_propagates(open_connection: MockType) -> None:
    # Given: A task waiting for the TCP connection to be established.
    open_connection.side_effect = stall
    opening = asyncio.create_task(
        AsyncConnection.open(
            ADDRESS,
            connection_timeout=5,
            socket_timeout=SOCKET_TIMEOUT,
        )
    )
    await asyncio.sleep(0)

    # When: The task is cancelled.
    opening.cancel()

    # Then: The cancellation is propagated unchanged.
    with pytest.raises(asyncio.CancelledError):
        await opening


async def test_send_preserves_payload(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: A complete request already contains the protocol delimiters.
    payload = b"set key 0 0 5\r\nvalue\r\n"

    # When: The request is sent.
    await connection.send(payload)

    # Then: All bytes are written and flushed without changing delimiters.
    writer.write.assert_called_once_with(payload)
    writer.drain.assert_awaited_once_with()
    writer.transport.abort.assert_not_called()
    assert not connection.broken


async def test_send_timeout_aborts_connection(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: The peer will not accept data within the socket timeout.
    writer.drain.side_effect = stall

    # When: The request is sent.
    with pytest.raises(ArcusOperationTimeoutError) as exc_info:
        await connection.send(b"get key\r\n")

    # Then: The connection is aborted and the timeout is the cause.
    assert connection.broken
    writer.transport.abort.assert_called_once_with()
    assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_send_failure_aborts_connection(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: The connection was reset by the peer.
    error = ConnectionResetError()
    writer.drain.side_effect = error

    # When: The request is sent.
    with pytest.raises(ArcusConnectionError) as exc_info:
        await connection.send(b"get key\r\n")

    # Then: The connection is aborted and the original cause is preserved.
    assert connection.broken
    writer.transport.abort.assert_called_once_with()
    assert exc_info.value.__cause__ is error


async def test_send_cancellation_aborts_connection(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: A task waiting for the send buffer to drain.
    writer.drain.side_effect = stall
    sending = asyncio.create_task(connection.send(b"get key\r\n"))
    await asyncio.sleep(0)

    # When: The task is cancelled.
    sending.cancel()

    # Then: The cancellation is propagated unchanged and the connection is aborted.
    with pytest.raises(asyncio.CancelledError):
        await sending
    assert connection.broken
    writer.transport.abort.assert_called_once_with()


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(None, id="success"),
        pytest.param(ConnectionResetError(), id="connection-lost-with-error"),
    ],
)
async def test_close_marks_connection_broken(
    connection: AsyncConnection,
    writer: MockType,
    error: OSError | None,
) -> None:
    # Given: The connection may have been lost with an error.
    writer.wait_closed.side_effect = error

    # When: The connection is closed twice.
    await connection.close()
    await connection.close()

    # Then: The connection is marked broken without propagating errors.
    assert connection.broken
    writer.close.assert_called_with()
    writer.wait_closed.assert_awaited_with()


async def test_abort_closes_transport(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: An open connection.

    # When: The connection is aborted.
    connection.abort()

    # Then: The transport is closed without waiting.
    writer.transport.abort.assert_called_once_with()
    writer.close.assert_not_called()
    assert connection.broken


def read_line() -> Exchange[bytes]:
    line = yield ReadLine()
    return line


def split(data: bytes, size: int | None) -> list[bytes]:
    if size is None:
        return [data]
    return [data[i : i + size] for i in range(0, len(data), size)]


async def feed(reader: asyncio.StreamReader, chunks: list[bytes]) -> None:
    """Deliver each chunk on its own loop iteration, then EOF."""
    for chunk in chunks:
        reader.feed_data(chunk)
        await asyncio.sleep(0)
    reader.feed_eof()


@pytest.mark.parametrize("chunk_size", [1, 3, None], ids=["1", "3", "all"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
async def test_execute_response_cases(
    connection: AsyncConnection,
    reader: asyncio.StreamReader,
    writer: MockType,
    case: Case,
    chunk_size: int | None,
) -> None:
    # Given: The response arrives in chunks, followed by EOF.
    feeding = asyncio.create_task(feed(reader, split(case.response, chunk_size)))

    # When: The exchange is executed.
    if case.error is None:
        result = await connection.execute(case.make())

        # Then: The result is returned and the connection stays usable.
        assert result == case.result
        assert not connection.broken
        writer.transport.abort.assert_not_called()
    else:
        with pytest.raises(case.error):
            await connection.execute(case.make())

        # Then: The connection is discarded.
        assert connection.broken
        writer.transport.abort.assert_called()

    await feeding


@pytest.mark.parametrize(
    "chunks",
    [
        pytest.param([b"x" * 4096 + b"\r\n"], id="one-chunk"),
        pytest.param([b"x" * 4096, b"x" * 4096 + b"\r", b"\n"], id="split-crlf"),
    ],
)
async def test_execute_reads_line_across_chunks(
    connection: AsyncConnection,
    reader: asyncio.StreamReader,
    chunks: list[bytes],
) -> None:
    # Given: A line whose terminator may arrive split across chunks.
    feeding = asyncio.create_task(feed(reader, chunks))

    # When: The line is read.
    line = await connection.execute(read_line())

    # Then: The line is returned without its terminator.
    assert line == b"".join(chunks)[:-2]
    assert not connection.broken
    await feeding


@pytest.mark.parametrize(
    "fail",
    [
        pytest.param(lambda reader: reader.feed_eof(), id="eof"),
        pytest.param(
            lambda reader: reader.set_exception(ConnectionResetError()),
            id="reset",
        ),
        pytest.param(
            lambda reader: reader.feed_data(b"x" * 2**17),
            id="line-too-long",
        ),
    ],
)
async def test_execute_read_failure_aborts_connection(
    connection: AsyncConnection,
    reader: asyncio.StreamReader,
    writer: MockType,
    fail: Callable[[asyncio.StreamReader], None],
) -> None:
    # Given: Reading the response will fail.
    fail(reader)

    # When: The exchange is executed.
    with pytest.raises(ArcusConnectionError):
        await connection.execute(read_line())

    # Then: The connection is aborted.
    assert connection.broken
    writer.transport.abort.assert_called()


async def test_execute_read_timeout_aborts_connection(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: No response arrives within the socket timeout.

    # When: The exchange is executed.
    with pytest.raises(ArcusOperationTimeoutError) as exc_info:
        await connection.execute(read_line())

    # Then: The connection is aborted and the timeout is the cause.
    assert connection.broken
    writer.transport.abort.assert_called()
    assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_execute_cancellation_aborts_connection(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: A task waiting for a response.
    executing = asyncio.create_task(connection.execute(read_line()))
    await asyncio.sleep(0)

    # When: The task is cancelled.
    executing.cancel()

    # Then: The cancellation is propagated unchanged and the connection is aborted.
    with pytest.raises(asyncio.CancelledError):
        await executing
    assert connection.broken
    writer.transport.abort.assert_called()


async def test_execute_on_broken_connection_sends_nothing(
    connection: AsyncConnection,
    writer: MockType,
) -> None:
    # Given: A connection that is already broken.
    connection.broken = True

    # When: An exchange is executed.
    with pytest.raises(ArcusConnectionError):
        await connection.execute(prepare_get(b"k", T))

    # Then: Nothing is sent.
    writer.write.assert_not_called()


async def test_execute_failure_closes_exchange(connection: AsyncConnection) -> None:
    # Given: An exchange that records its cleanup, and a response that times out.
    closed = False

    def exchange() -> Exchange[bytes]:
        nonlocal closed
        try:
            line = yield ReadLine()
            return line
        finally:
            closed = True

    # When: The exchange is executed.
    with pytest.raises(ArcusOperationTimeoutError):
        await connection.execute(exchange())

    # Then: The exchange is closed and the connection is discarded.
    assert closed
    assert connection.broken


async def test_execute_partial_response_timeout_closes_socket() -> None:
    # Given: A peer that sends only part of the response, then goes silent.
    connection, peer = await open_pair(socket_timeout=SOCKET_TIMEOUT)
    loop = asyncio.get_running_loop()

    with peer:
        executing = asyncio.create_task(connection.execute(prepare_get(b"k", T)))
        assert await loop.sock_recv(peer, 64) == b"get k\r\n"
        await loop.sock_sendall(peer, b'VALUE k 0 3\r\n"v')

        # When: The rest of the response does not arrive in time.
        with pytest.raises(ArcusOperationTimeoutError):
            await executing

        # Then: The socket is closed so the late bytes cannot be read later.
        assert connection.broken
        assert await loop.sock_recv(peer, 64) == b""


async def test_cancel_after_send_closes_socket() -> None:
    # Given: A task waiting for a response to a request it has sent.
    connection, peer = await open_pair()
    loop = asyncio.get_running_loop()

    with peer:
        executing = asyncio.create_task(connection.execute(prepare_get(b"k", T)))
        assert await loop.sock_recv(peer, 64) == b"get k\r\n"

        # When: The task is cancelled.
        executing.cancel()

        # Then: The cancellation is propagated and the socket is closed.
        with pytest.raises(asyncio.CancelledError):
            await executing
        assert connection.broken
        assert await loop.sock_recv(peer, 64) == b""


async def test_abort_wakes_blocked_execute() -> None:
    # Given: A task blocked waiting for a response from a silent peer.
    connection, peer = await open_pair()
    loop = asyncio.get_running_loop()

    with peer:
        executing = asyncio.create_task(connection.execute(prepare_get(b"k", T)))
        assert await loop.sock_recv(peer, 64) == b"get k\r\n"

        # When: Another task aborts the connection.
        connection.abort()

        # Then: The request fails at once instead of waiting for the timeout.
        with pytest.raises(ArcusConnectionError):
            await asyncio.wait_for(executing, timeout=1)
