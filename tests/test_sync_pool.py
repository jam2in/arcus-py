import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import cast

import pytest
from pytest_mock import MockerFixture, MockType

from arcus import ArcusConnectionError, ArcusError, ArcusPoolTimeoutError
from arcus._transport.sync.connection import SyncConnection
from arcus._transport.sync.pool import SyncConnectionPool

ADDRESS = "127.0.0.1:11211"
# Long enough for a waiting thread to block before the main thread acts.
SETTLE = 0.05


@pytest.fixture
def connect(mocker: MockerFixture) -> MockType:
    def open_connection(address: str) -> SyncConnection:
        return SyncConnection(address, mocker.Mock(spec=socket.socket))

    return mocker.Mock(side_effect=open_connection)


def make_pool(
    connect: MockType,
    *,
    max_size: int = 2,
    # Well beyond the 1s result waits, so a missed wake-up fails the test.
    pool_timeout: float = 5.0,
) -> SyncConnectionPool:
    return SyncConnectionPool(
        ADDRESS,
        max_size=max_size,
        pool_timeout=pool_timeout,
        connect=connect,
    )


def sock_of(connection: SyncConnection) -> MockType:
    return cast(MockType, connection._socket)


def test_no_connection_until_acquire(connect: MockType) -> None:
    # Given / When: A pool is created.
    pool = make_pool(connect)

    # Then: No connection is opened.
    connect.assert_not_called()
    assert pool.in_use == 0
    assert pool.idle == 0


def test_released_connection_is_reused(connect: MockType) -> None:
    # Given: A connection that was acquired and released.
    pool = make_pool(connect)
    first = pool.acquire()
    pool.release(first)

    # When: A connection is acquired again.
    second = pool.acquire()

    # Then: The same connection is lent without connecting again.
    assert second is first
    connect.assert_called_once_with(ADDRESS)
    assert pool.in_use == 1
    assert pool.idle == 0


def test_most_recently_released_connection_is_reused_first(
    connect: MockType,
) -> None:
    # Given: Two connections released one after another.
    pool = make_pool(connect)
    first = pool.acquire()
    second = pool.acquire()
    pool.release(first)
    pool.release(second)

    # When: A connection is acquired.
    connection = pool.acquire()

    # Then: The last released one is lent.
    assert connection is second


def test_broken_connection_is_closed_on_release(connect: MockType) -> None:
    # Given: An acquired connection that became broken.
    pool = make_pool(connect, max_size=1)
    broken = pool.acquire()
    broken.broken = True

    # When: It is released and another connection is acquired.
    pool.release(broken)
    connection = pool.acquire()

    # Then: The broken one is closed and a new one is opened.
    sock_of(broken).close.assert_called_once()
    assert connection is not broken
    assert connect.call_count == 2


def test_acquire_times_out_when_pool_is_full(connect: MockType) -> None:
    # Given: A full pool.
    pool = make_pool(connect, max_size=1, pool_timeout=SETTLE)
    pool.acquire()

    # When / Then: Another acquire fails after the pool timeout.
    with pytest.raises(ArcusPoolTimeoutError):
        pool.acquire()
    assert pool.in_use == 1


def test_waiter_gets_released_connection(connect: MockType) -> None:
    # Given: A full pool and a thread waiting for a connection.
    pool = make_pool(connect, max_size=1)
    held = pool.acquire()

    with ThreadPoolExecutor(1) as executor:
        waiter = executor.submit(pool.acquire)
        time.sleep(SETTLE)

        # When: The held connection is released.
        pool.release(held)

        # Then: The waiter receives it.
        assert waiter.result(timeout=1) is held
    connect.assert_called_once()


def test_connect_failure_restores_capacity(connect: MockType) -> None:
    # Given: A pool whose node refuses connections three times.
    pool = make_pool(connect, max_size=1)
    refused = ArcusConnectionError("refused")
    connected = connect.side_effect(ADDRESS)
    connect.side_effect = [refused, refused, refused, connected]

    # When: Acquire is attempted repeatedly.
    for _ in range(3):
        with pytest.raises(ArcusConnectionError):
            pool.acquire()
    connection = pool.acquire()

    # Then: Each failure returned its capacity.
    assert connection is connected
    assert pool.in_use == 1


def test_connect_failure_wakes_waiter(connect: MockType) -> None:
    # Given: A slow connect that will fail, and a second thread waiting.
    pool = make_pool(connect, max_size=1)
    proceed = threading.Event()
    open_connection = connect.side_effect

    def fail_once(address: str) -> SyncConnection:
        connect.side_effect = open_connection
        proceed.wait(1)
        raise ArcusConnectionError("refused")

    connect.side_effect = fail_once

    with ThreadPoolExecutor(2) as executor:
        failing = executor.submit(pool.acquire)
        time.sleep(SETTLE)
        waiter = executor.submit(pool.acquire)
        time.sleep(SETTLE)

        # When: The first connect fails.
        proceed.set()

        # Then: The waiter takes the freed capacity and connects.
        with pytest.raises(ArcusConnectionError):
            failing.result(timeout=1)
        assert isinstance(waiter.result(timeout=1), SyncConnection)
    assert pool.in_use == 1


def test_slow_connect_does_not_block_release(connect: MockType) -> None:
    # Given: One held connection and another thread stuck in connect.
    pool = make_pool(connect, max_size=2)
    held = pool.acquire()
    entered = threading.Event()
    proceed = threading.Event()
    open_connection = connect.side_effect

    def slow_connect(address: str) -> SyncConnection:
        entered.set()
        proceed.wait()
        return open_connection(address)

    def release_and_acquire() -> SyncConnection:
        pool.release(held)
        return pool.acquire()

    connect.side_effect = slow_connect

    with ThreadPoolExecutor(2) as executor:
        connecting = executor.submit(pool.acquire)
        try:
            assert entered.wait(1)

            # When: The held connection is released and acquired again.
            reusing = executor.submit(release_and_acquire)

            # Then: Both finish while the connect is still in progress.
            assert reusing.result(timeout=1) is held
            assert not connecting.done()
        finally:
            proceed.set()
        connecting.result(timeout=1)


def test_close_wakes_waiters_with_reason(connect: MockType) -> None:
    # Given: A full pool and a thread waiting for a connection.
    pool = make_pool(connect, max_size=1)
    pool.acquire()

    with ThreadPoolExecutor(1) as executor:
        waiter = executor.submit(pool.acquire)
        time.sleep(SETTLE)

        # When: The pool is closed.
        pool.close(ArcusError("closed"), abort_in_use=False)

        # Then: The waiter fails with the close reason.
        with pytest.raises(ArcusError, match="closed"):
            waiter.result(timeout=1)


def test_acquire_after_close_raises_reason(connect: MockType) -> None:
    # Given: A closed pool.
    pool = make_pool(connect)
    pool.close(ArcusConnectionError("closed"), abort_in_use=False)

    # When / Then: Acquire fails with the close reason without connecting.
    with pytest.raises(ArcusConnectionError, match="closed"):
        pool.acquire()
    connect.assert_not_called()


def test_close_closes_idle_connections(connect: MockType) -> None:
    # Given: A pool with an idle connection.
    pool = make_pool(connect)
    connection = pool.acquire()
    pool.release(connection)

    # When: The pool is closed.
    pool.close(ArcusError("closed"), abort_in_use=False)

    # Then: The idle connection is closed.
    sock_of(connection).close.assert_called_once()
    assert pool.idle == 0


def test_release_after_close_closes_connection(connect: MockType) -> None:
    # Given: A connection in use while the pool is closed.
    pool = make_pool(connect)
    connection = pool.acquire()
    pool.close(ArcusError("closed"), abort_in_use=False)

    # When: The connection is released.
    pool.release(connection)

    # Then: It is closed instead of kept idle.
    sock_of(connection).close.assert_called_once()
    assert pool.in_use == 0
    assert pool.idle == 0


def test_close_aborts_in_use_connections(connect: MockType) -> None:
    # Given: A connection in use.
    pool = make_pool(connect)
    connection = pool.acquire()

    # When: The pool is closed with abort_in_use.
    pool.close(ArcusError("closed"), abort_in_use=True)

    # Then: The connection is shut down so a blocked request wakes up.
    sock_of(connection).shutdown.assert_called_once_with(socket.SHUT_RDWR)
    assert connection.broken


def test_close_leaves_in_use_connections_running(connect: MockType) -> None:
    # Given: A connection in use.
    pool = make_pool(connect)
    connection = pool.acquire()

    # When: The pool is closed without abort_in_use.
    pool.close(ArcusError("closed"), abort_in_use=False)

    # Then: The in-flight request can finish on it until it is released.
    sock_of(connection).shutdown.assert_not_called()
    sock_of(connection).close.assert_not_called()
    assert not connection.broken


def test_close_is_idempotent(connect: MockType) -> None:
    # Given: A closed pool.
    pool = make_pool(connect)
    pool.close(ArcusError("first"), abort_in_use=False)

    # When: It is closed again with another reason.
    pool.close(ArcusError("second"), abort_in_use=False)

    # Then: The first reason is kept.
    with pytest.raises(ArcusError, match="first"):
        pool.acquire()


def test_connect_finished_after_close_is_closed(connect: MockType) -> None:
    # Given: A connect that finishes after the pool is closed.
    pool = make_pool(connect)
    proceed = threading.Event()
    opened: list[SyncConnection] = []
    open_connection = connect.side_effect

    def slow_connect(address: str) -> SyncConnection:
        proceed.wait(1)
        connection = open_connection(address)
        opened.append(connection)
        return connection

    connect.side_effect = slow_connect

    with ThreadPoolExecutor(1) as executor:
        connecting = executor.submit(pool.acquire)
        time.sleep(SETTLE)
        pool.close(ArcusError("closed"), abort_in_use=False)

        # When: The connect finishes.
        proceed.set()

        # Then: The new connection is closed and the caller gets the reason.
        with pytest.raises(ArcusError, match="closed"):
            connecting.result(timeout=1)
    sock_of(opened[0]).close.assert_called_once()
    assert pool.in_use == 0
