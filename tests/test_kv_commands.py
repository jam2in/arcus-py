from collections.abc import Callable
from typing import TypeVar

import pytest
from support.drive import drive

from arcus._command.kv import KVCommands
from arcus._protocol.request import Exchange
from arcus.transcoder import ArcusTranscoder

T = TypeVar("T")


class StubClient(KVCommands):
    """Answer every request with a fixed response, without sockets."""

    def __init__(self, response: bytes) -> None:
        self._transcoder = ArcusTranscoder()
        self.response = response
        self.calls: list[tuple[bytes, bytes]] = []

    def _execute(self, key: bytes, exchange: Exchange[T]) -> T:
        result, sent = drive(exchange, self.response)
        self.calls.append((key, sent))
        return result


def test_get_routes_by_encoded_key() -> None:
    # Given: A client whose node holds the value.
    client = StubClient(b'VALUE \xea\xb0\x80 0 3\r\n"v"\r\nEND\r\n')

    # When: The value is read.
    result = client.get("가")

    # Then: The UTF-8 key is used for routing and the request.
    assert result == "v"
    assert client.calls == [("가".encode(), "get 가\r\n".encode())]


def test_get_miss_returns_none() -> None:
    # Given: A client whose node does not hold the key.
    client = StubClient(b"END\r\n")

    # When / Then: The miss is returned as None.
    assert client.get("k") is None


def test_set_sends_value_with_default_exptime() -> None:
    # Given: A client whose node stores the value.
    client = StubClient(b"STORED\r\n")

    # When: A value is stored without an exptime.
    result = client.set("k", {"a": 1})

    # Then: The JSON value is sent with exptime 0.
    assert result is True
    assert client.calls == [(b"k", b'set k 0 0 8\r\n{"a": 1}\r\n')]


@pytest.mark.parametrize(
    ("call", "error"),
    [
        pytest.param(lambda c: c.get(""), ValueError, id="get-empty-key"),
        pytest.param(lambda c: c.get(b"k"), TypeError, id="get-bytes-key"),
        pytest.param(lambda c: c.set("a b", "v"), ValueError, id="set-space-key"),
        pytest.param(lambda c: c.set("k", None), TypeError, id="set-none-value"),
        pytest.param(lambda c: c.set("k", b"v"), TypeError, id="set-bytes-value"),
        pytest.param(lambda c: c.set("k", "v", -2), ValueError, id="set-below-sticky"),
        pytest.param(lambda c: c.set("k", "v", True), TypeError, id="set-bool"),
    ],
)
def test_invalid_input_fails_before_execute(
    call: Callable[[StubClient], object],
    error: type[Exception],
) -> None:
    # Given: A client that would record any request it executes.
    client = StubClient(b"STORED\r\n")

    # When: A command is called with invalid input.
    with pytest.raises(error):
        call(client)

    # Then: Nothing reaches the executor.
    assert client.calls == []


def test_executor_without_execute_cannot_be_created() -> None:
    # Given: A command mixin user that forgot to implement _execute.
    class Incomplete(KVCommands):
        pass

    # When / Then: Creating it fails instead of failing on the first request.
    with pytest.raises(TypeError, match="_execute"):
        Incomplete()  # ty: ignore[call-non-callable]
