import asyncio
from typing import cast

import pytest
from pytest_mock import MockerFixture, MockType

from arcus import ArcusConnectionError, ArcusError, ArcusPoolTimeoutError
from arcus._transport.aio.connection import AsyncConnection
from arcus._transport.aio.pool import AsyncConnectionPool

ADDRESS = "127.0.0.1:11211"
# Long enough for a waiting task to block before the test acts.
SETTLE = 0.05


@pytest.fixture
def connect(mocker: MockerFixture) -> MockType:
    async def open_connection(address: str) -> AsyncConnection:
        reader = mocker.Mock(spec=asyncio.StreamReader)
        writer = mocker.Mock(spec=asyncio.StreamWriter)
        return AsyncConnection(address, reader, writer, socket_timeout=5)

    return mocker.AsyncMock(side_effect=open_connection)


def make_pool(
    connect: MockType,
    *,
    max_size: int = 2,
    # Well beyond the 1s result waits, so a missed wake-up fails the test.
    pool_timeout: float = 5.0,
) -> AsyncConnectionPool:
    return AsyncConnectionPool(
        ADDRESS,
        max_size=max_size,
        pool_timeout=pool_timeout,
        connect=connect,
    )


def transport_of(connection: AsyncConnection) -> MockType:
    return cast(MockType, connection._writer.transport)


def stall_first_connect(connect: MockType) -> None:
    """Make the next connect hang until cancelled; later ones connect at once."""
    open_connection = connect.side_effect

    async def stall(address: str) -> AsyncConnection:
        connect.side_effect = open_connection
        await asyncio.sleep(10)
        raise AssertionError("connect was expected to be cancelled")

    connect.side_effect = stall


async def test_no_connection_until_acquire(connect: MockType) -> None:
    # Given / When: A pool is created.
    pool = make_pool(connect)

    # Then: No connection is opened.
    connect.assert_not_called()
    assert pool.in_use == 0
    assert pool.idle == 0


async def test_released_connection_is_reused(connect: MockType) -> None:
    # Given: A connection that was acquired and released.
    pool = make_pool(connect)
    first = await pool.acquire()
    pool.release(first)

    # When: A connection is acquired again.
    second = await pool.acquire()

    # Then: The same connection is lent without connecting again.
    assert second is first
    connect.assert_awaited_once_with(ADDRESS)
    assert pool.in_use == 1
    assert pool.idle == 0


async def test_most_recently_released_connection_is_reused_first(
    connect: MockType,
) -> None:
    # Given: Two connections released one after another.
    pool = make_pool(connect)
    first = await pool.acquire()
    second = await pool.acquire()
    pool.release(first)
    pool.release(second)

    # When: A connection is acquired.
    connection = await pool.acquire()

    # Then: The last released one is lent.
    assert connection is second


async def test_broken_connection_is_aborted_on_release(connect: MockType) -> None:
    # Given: An acquired connection that became broken.
    pool = make_pool(connect, max_size=1)
    broken = await pool.acquire()
    broken.broken = True

    # When: It is released and another connection is acquired.
    pool.release(broken)
    connection = await pool.acquire()

    # Then: The broken one is aborted and a new one is opened.
    transport_of(broken).abort.assert_called_once()
    assert connection is not broken
    assert connect.await_count == 2


async def test_acquire_times_out_when_pool_is_full(connect: MockType) -> None:
    # Given: A full pool.
    pool = make_pool(connect, max_size=1, pool_timeout=SETTLE)
    await pool.acquire()

    # When / Then: Another acquire fails after the pool timeout.
    with pytest.raises(ArcusPoolTimeoutError):
        await pool.acquire()
    assert pool.in_use == 1


async def test_zero_pool_timeout_fails_without_waiting(connect: MockType) -> None:
    # Given: A full pool that does not wait for a connection.
    pool = make_pool(connect, max_size=1, pool_timeout=0)
    await pool.acquire()

    # When / Then: Another acquire fails at once.
    with pytest.raises(ArcusPoolTimeoutError):
        await pool.acquire()


async def test_waiter_gets_released_connection(connect: MockType) -> None:
    # Given: A full pool and a task waiting for a connection.
    pool = make_pool(connect, max_size=1)
    held = await pool.acquire()
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The held connection is released.
    pool.release(held)

    # Then: The waiter receives it.
    assert await asyncio.wait_for(waiter, 1) is held
    connect.assert_awaited_once()


async def test_waiters_are_served_in_arrival_order(connect: MockType) -> None:
    # Given: A full pool and two tasks waiting one after another.
    pool = make_pool(connect, max_size=1)
    held = await pool.acquire()
    first = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)
    second = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The held connection is released once.
    pool.release(held)

    # Then: Only the first waiter receives it.
    assert await asyncio.wait_for(first, 1) is held
    assert not second.done()
    pool.release(held)
    assert await asyncio.wait_for(second, 1) is held


async def test_connect_failure_restores_capacity(connect: MockType) -> None:
    # Given: A pool whose node refuses connections three times.
    pool = make_pool(connect, max_size=1)
    refused = ArcusConnectionError("refused")
    connected = await connect.side_effect(ADDRESS)
    connect.side_effect = [refused, refused, refused, connected]

    # When: Acquire is attempted repeatedly.
    for _ in range(3):
        with pytest.raises(ArcusConnectionError):
            await pool.acquire()
    connection = await pool.acquire()

    # Then: Each failure returned its capacity.
    assert connection is connected
    assert pool.in_use == 1


async def test_connect_failure_wakes_waiter(connect: MockType) -> None:
    # Given: A slow connect that will fail, and a second task waiting.
    pool = make_pool(connect, max_size=1)
    proceed = asyncio.Event()
    open_connection = connect.side_effect

    async def fail_once(address: str) -> AsyncConnection:
        connect.side_effect = open_connection
        await proceed.wait()
        raise ArcusConnectionError("refused")

    connect.side_effect = fail_once
    failing = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The first connect fails.
    proceed.set()

    # Then: The waiter takes the freed capacity and connects.
    with pytest.raises(ArcusConnectionError):
        await asyncio.wait_for(failing, 1)
    assert isinstance(await asyncio.wait_for(waiter, 1), AsyncConnection)
    assert pool.in_use == 1


async def test_slow_connect_does_not_block_release(connect: MockType) -> None:
    # Given: One held connection and another task stuck in connect.
    pool = make_pool(connect, max_size=2)
    held = await pool.acquire()
    proceed = asyncio.Event()
    open_connection = connect.side_effect

    async def slow_connect(address: str) -> AsyncConnection:
        await proceed.wait()
        return await open_connection(address)

    connect.side_effect = slow_connect
    connecting = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    try:
        # When: The held connection is released and acquired again.
        pool.release(held)
        reused = await asyncio.wait_for(pool.acquire(), 1)

        # Then: Both finish while the connect is still in progress.
        assert reused is held
        assert not connecting.done()
    finally:
        proceed.set()
    await asyncio.wait_for(connecting, 1)


async def test_cancel_during_connect_restores_capacity(connect: MockType) -> None:
    # Given: A task stuck in connect.
    pool = make_pool(connect, max_size=1)
    stall_first_connect(connect)
    connecting = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The task is cancelled.
    connecting.cancel()

    # Then: Its capacity is returned and the next acquire connects.
    with pytest.raises(asyncio.CancelledError):
        await connecting
    assert pool.in_use == 0
    assert isinstance(await asyncio.wait_for(pool.acquire(), 1), AsyncConnection)


async def test_cancel_during_connect_wakes_waiter(connect: MockType) -> None:
    # Given: A task stuck in connect, and a second task waiting.
    pool = make_pool(connect, max_size=1)
    stall_first_connect(connect)
    connecting = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The connecting task is cancelled.
    connecting.cancel()

    # Then: The waiter takes the freed capacity and connects.
    assert isinstance(await asyncio.wait_for(waiter, 1), AsyncConnection)
    with pytest.raises(asyncio.CancelledError):
        await connecting


async def test_cancel_while_waiting_does_not_leak(connect: MockType) -> None:
    # Given: A full pool and a task waiting for a connection.
    pool = make_pool(connect, max_size=1)
    held = await pool.acquire()
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The waiting task is cancelled, then the connection is released.
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    pool.release(held)

    # Then: The next acquire gets the connection at once.
    assert await asyncio.wait_for(pool.acquire(), 1) is held
    assert pool.in_use == 1


async def test_cancel_after_wake_passes_turn_to_next_waiter(
    connect: MockType,
) -> None:
    # Given: A full pool and two tasks waiting one after another.
    pool = make_pool(connect, max_size=1)
    held = await pool.acquire()
    first = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)
    second = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The first waiter is woken, then cancelled before it runs.
    pool.release(held)
    first.cancel()

    # Then: The second waiter receives the connection.
    with pytest.raises(asyncio.CancelledError):
        await first
    assert await asyncio.wait_for(second, 1) is held


async def test_repeated_broken_releases_keep_capacity(connect: MockType) -> None:
    # Given: A pool with room for one connection.
    pool = make_pool(connect, max_size=1, pool_timeout=SETTLE)

    # When: Each connection breaks and is released, 50 times.
    for _ in range(50):
        connection = await pool.acquire()
        connection.broken = True
        pool.release(connection)

    # Then: Every acquire got capacity and none is left in use.
    assert connect.await_count == 50
    assert pool.in_use == 0
    assert pool.idle == 0


async def test_waiter_gets_released_connection_before_new_caller(
    connect: MockType,
) -> None:
    # Given: A full pool and a task waiting for a connection.
    pool = make_pool(connect, max_size=1, pool_timeout=SETTLE)
    held = await pool.acquire()
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0)

    # When: The holder releases the connection and acquires again at once.
    pool.release(held)
    with pytest.raises(ArcusPoolTimeoutError):
        await pool.acquire()

    # Then: The waiter receives the connection.
    assert await asyncio.wait_for(waiter, 1) is held


async def test_waiter_gets_freed_capacity_before_new_caller(
    connect: MockType,
) -> None:
    # Given: A full pool whose connection broke, and a task waiting.
    pool = make_pool(connect, max_size=1, pool_timeout=SETTLE)
    broken = await pool.acquire()
    broken.broken = True
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(0)

    # When: The holder releases it and acquires again at once.
    pool.release(broken)
    with pytest.raises(ArcusPoolTimeoutError):
        await pool.acquire()

    # Then: The waiter takes the freed capacity and connects.
    connection = await asyncio.wait_for(waiter, 1)
    assert connection is not broken


async def test_waiter_is_not_starved_by_reacquiring_task(connect: MockType) -> None:
    # Given: A task that keeps releasing and acquiring the only connection.
    pool = make_pool(connect, max_size=1, pool_timeout=0.5)
    stop = False

    async def reacquire_forever() -> None:
        while not stop:
            connection = await pool.acquire()
            await asyncio.sleep(0.001)
            pool.release(connection)

    loop = asyncio.create_task(reacquire_forever())
    await asyncio.sleep(SETTLE)

    # When: Another task acquires a connection.
    connection = await pool.acquire()

    # Then: It gets one within the pool timeout.
    stop = True
    pool.release(connection)
    await asyncio.wait_for(loop, 1)


async def test_released_connection_is_lent_while_woken_waiter_has_not_run(
    connect: MockType,
) -> None:
    # Given: A full pool of two and a task waiting for a connection.
    pool = make_pool(connect, max_size=2)
    first = await pool.acquire()
    second = await pool.acquire()
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: Both are released before the woken waiter runs, then acquired again.
    pool.release(first)
    pool.release(second)
    again = await asyncio.wait_for(pool.acquire(), 1)

    # Then: The waiter and the new caller each get a connection.
    assert await asyncio.wait_for(waiter, 1) is not again
    assert pool.in_use == 2


async def test_cancel_after_wake_passes_freed_capacity_on(
    connect: MockType,
) -> None:
    # Given: A full pool whose connection broke, and two tasks waiting.
    pool = make_pool(connect, max_size=1)
    broken = await pool.acquire()
    broken.broken = True
    first = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)
    second = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The first waiter is woken, then cancelled before it runs.
    pool.release(broken)
    first.cancel()

    # Then: The second waiter takes the freed capacity and connects.
    with pytest.raises(asyncio.CancelledError):
        await first
    assert isinstance(await asyncio.wait_for(second, 1), AsyncConnection)
    assert pool.in_use == 1


async def test_mixed_traffic_returns_all_capacity(connect: MockType) -> None:
    # Given: Many tasks that acquire, sometimes break, and sometimes cancel.
    pool = make_pool(connect, max_size=2, pool_timeout=1)

    async def request(i: int) -> None:
        connection = await pool.acquire()
        try:
            await asyncio.sleep(0.001 * (i % 3))
            connection.broken = i % 4 == 0
        finally:
            pool.release(connection)

    tasks = [asyncio.create_task(request(i)) for i in range(60)]
    await asyncio.sleep(0.005)

    # When: Some of them are cancelled while the rest finish.
    for task in tasks[5::7]:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Then: Nothing is left in use, and capacity is still available.
    assert pool.in_use == 0
    assert pool.idle <= 2
    assert isinstance(await asyncio.wait_for(pool.acquire(), 1), AsyncConnection)


async def test_close_wakes_waiters_with_reason(connect: MockType) -> None:
    # Given: A full pool and tasks waiting for a connection.
    pool = make_pool(connect, max_size=1)
    await pool.acquire()
    waiters = [asyncio.create_task(pool.acquire()) for _ in range(3)]
    await asyncio.sleep(SETTLE)

    # When: The pool is closed.
    pool.close(ArcusError("closed"), abort_in_use=False)

    # Then: Every waiter fails with the close reason.
    for waiter in waiters:
        with pytest.raises(ArcusError, match="closed"):
            await asyncio.wait_for(waiter, 1)


async def test_acquire_after_close_raises_reason(connect: MockType) -> None:
    # Given: A closed pool.
    pool = make_pool(connect)
    pool.close(ArcusConnectionError("closed"), abort_in_use=False)

    # When / Then: Acquire fails with the close reason without connecting.
    with pytest.raises(ArcusConnectionError, match="closed"):
        await pool.acquire()
    connect.assert_not_called()


async def test_close_aborts_idle_connections(connect: MockType) -> None:
    # Given: A pool with an idle connection.
    pool = make_pool(connect)
    connection = await pool.acquire()
    pool.release(connection)

    # When: The pool is closed.
    pool.close(ArcusError("closed"), abort_in_use=False)

    # Then: The idle connection is aborted.
    transport_of(connection).abort.assert_called_once()
    assert pool.idle == 0


async def test_release_after_close_aborts_connection(connect: MockType) -> None:
    # Given: A connection in use while the pool is closed.
    pool = make_pool(connect)
    connection = await pool.acquire()
    pool.close(ArcusError("closed"), abort_in_use=False)

    # When: The connection is released.
    pool.release(connection)

    # Then: It is aborted instead of kept idle.
    transport_of(connection).abort.assert_called_once()
    assert pool.in_use == 0
    assert pool.idle == 0


async def test_close_aborts_in_use_connections(connect: MockType) -> None:
    # Given: A connection in use.
    pool = make_pool(connect)
    connection = await pool.acquire()

    # When: The pool is closed with abort_in_use.
    pool.close(ArcusError("closed"), abort_in_use=True)

    # Then: The connection is aborted so a blocked request wakes up.
    transport_of(connection).abort.assert_called_once()
    assert connection.broken


async def test_close_leaves_in_use_connections_running(connect: MockType) -> None:
    # Given: A connection in use.
    pool = make_pool(connect)
    connection = await pool.acquire()

    # When: The pool is closed without abort_in_use.
    pool.close(ArcusError("closed"), abort_in_use=False)

    # Then: The in-flight request can finish on it until it is released.
    transport_of(connection).abort.assert_not_called()
    assert not connection.broken


async def test_close_is_idempotent(connect: MockType) -> None:
    # Given: A closed pool.
    pool = make_pool(connect)
    pool.close(ArcusError("first"), abort_in_use=False)

    # When: It is closed again with another reason.
    pool.close(ArcusError("second"), abort_in_use=False)

    # Then: The first reason is kept.
    with pytest.raises(ArcusError, match="first"):
        await pool.acquire()


async def test_close_again_aborts_connections_left_running(
    connect: MockType,
) -> None:
    # Given: A pool closed without aborting a connection in use.
    pool = make_pool(connect)
    connection = await pool.acquire()
    pool.close(ArcusError("first"), abort_in_use=False)

    # When: It is closed again with abort_in_use.
    pool.close(ArcusError("second"), abort_in_use=True)

    # Then: The connection is aborted, and the first reason is kept.
    transport_of(connection).abort.assert_called_once()
    assert connection.broken
    with pytest.raises(ArcusError, match="first"):
        await pool.acquire()


async def test_close_after_release_to_waiter_fails_waiter(connect: MockType) -> None:
    # Given: A full pool and a task waiting for a connection.
    pool = make_pool(connect, max_size=1)
    held = await pool.acquire()
    waiter = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)

    # When: The connection is released, then the pool closes before the waiter runs.
    pool.release(held)
    pool.close(ArcusError("closed"), abort_in_use=False)

    # Then: The waiter fails with the close reason and the connection is aborted.
    with pytest.raises(ArcusError, match="closed"):
        await asyncio.wait_for(waiter, 1)
    transport_of(held).abort.assert_called_once()
    assert pool.in_use == 0
    assert pool.idle == 0


async def test_connect_finished_after_close_is_aborted(connect: MockType) -> None:
    # Given: A connect that finishes after the pool is closed.
    pool = make_pool(connect)
    proceed = asyncio.Event()
    opened: list[AsyncConnection] = []
    open_connection = connect.side_effect

    async def slow_connect(address: str) -> AsyncConnection:
        await proceed.wait()
        connection = await open_connection(address)
        opened.append(connection)
        return connection

    connect.side_effect = slow_connect
    connecting = asyncio.create_task(pool.acquire())
    await asyncio.sleep(SETTLE)
    pool.close(ArcusError("closed"), abort_in_use=False)

    # When: The connect finishes.
    proceed.set()

    # Then: The new connection is aborted and the caller gets the reason.
    with pytest.raises(ArcusError, match="closed"):
        await asyncio.wait_for(connecting, 1)
    transport_of(opened[0]).abort.assert_called_once()
    assert pool.in_use == 0
