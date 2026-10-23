"""Response cases shared by the protocol tests and the connection drivers."""

from collections.abc import Callable
from dataclasses import dataclass

from arcus import (
    ArcusClientError,
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusError,
    ArcusServerError,
)
from arcus._command.kv import prepare_get, prepare_set
from arcus._protocol.request import Exchange
from arcus.transcoder import ArcusTranscoder

T = ArcusTranscoder()


@dataclass(frozen=True, slots=True)
class Case:
    name: str
    make: Callable[[], Exchange[object]]
    response: bytes
    result: object = None
    error: type[ArcusError] | None = None


def _get() -> Exchange[object]:
    return prepare_get("k", T)


def _set() -> Exchange[object]:
    return prepare_set("k", "v", 0, T)


CASES: list[Case] = [
    Case("get_hit", _get, b'VALUE k 0 3\r\n"v"\r\nEND\r\n', result="v"),
    Case("get_miss", _get, b"END\r\n"),
    Case(
        "get_dict",
        _get,
        b'VALUE k 0 8\r\n{"a": 1}\r\nEND\r\n',
        result={"a": 1},
    ),
    Case("get_empty_string", _get, b'VALUE k 0 2\r\n""\r\nEND\r\n', result=""),
    Case("get_ignores_flags", _get, b'VALUE k 512 3\r\n"v"\r\nEND\r\n', result="v"),
    Case(
        "get_bad_terminator",
        _get,
        b'VALUE k 0 3\r\n"v"XXEND\r\n',
        error=ArcusConnectionError,
    ),
    Case(
        "get_missing_end",
        _get,
        b'VALUE k 0 3\r\n"v"\r\nSTORED\r\n',
        error=ArcusConnectionError,
    ),
    Case(
        "get_wrong_key",
        _get,
        b'VALUE other 0 3\r\n"v"\r\nEND\r\n',
        error=ArcusConnectionError,
    ),
    Case(
        "get_bad_length",
        _get,
        b"VALUE k 0 x\r\n",
        error=ArcusConnectionError,
    ),
    Case(
        "get_decode_error",
        _get,
        b"VALUE k 0 3\r\nnot\r\nEND\r\n",
        error=ArcusDecodeError,
    ),
    Case(
        "get_short_eof",
        _get,
        b'VALUE k 0 5\r\n"ab',
        error=ArcusConnectionError,
    ),
    Case("set_stored", _set, b"STORED\r\n", result=True),
    Case("set_type_mismatch", _set, b"TYPE_MISMATCH\r\n", result=False),
    Case(
        "server_error",
        _set,
        b"SERVER_ERROR out of memory\r\n",
        error=ArcusServerError,
    ),
    Case(
        "client_error",
        _set,
        b"CLIENT_ERROR bad data chunk\r\n",
        error=ArcusClientError,
    ),
    Case("error", _set, b"ERROR\r\n", error=ArcusError),
    Case(
        "unknown_line",
        _set,
        b"WHAT\r\n",
        error=ArcusConnectionError,
    ),
]
