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
from arcus._command.vector import (
    VectorOptions,
    prepare_vop_create,
    prepare_vop_search,
    prepare_vop_upsert,
)
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
    return prepare_get(b"k", T)


def _set() -> Exchange[object]:
    return prepare_set(b"k", "v", 0, T)


def _vop_create() -> Exchange[object]:
    return prepare_vop_create(b"k", VectorOptions(dim=3))


def _vop_upsert() -> Exchange[object]:
    return prepare_vop_upsert(b"k", "v", [1.0], None)


def _vop_search() -> Exchange[object]:
    return prepare_vop_search(b"k", "v", 2, None, False)


def _vop_search_with_meta() -> Exchange[object]:
    return prepare_vop_search(b"k", "v", 2, None, True)


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
    Case("vop_create_created", _vop_create, b"CREATED\r\n", result=True),
    Case("vop_create_exists", _vop_create, b"EXISTS\r\n", result=False),
    Case(
        "vop_create_bad_command_line",
        _vop_create,
        b"CLIENT_ERROR bad command line format\r\n",
        error=ArcusClientError,
    ),
    Case("vop_upsert_stored", _vop_upsert, b"STORED\r\n", result=True),
    Case("vop_upsert_replaced", _vop_upsert, b"REPLACED\r\n", result=True),
    Case("vop_upsert_not_found", _vop_upsert, b"NOT_FOUND\r\n", result=False),
    Case(
        "vop_upsert_type_mismatch",
        _vop_upsert,
        b"TYPE_MISMATCH\r\n",
        result=False,
    ),
    Case("vop_upsert_overflowed", _vop_upsert, b"OVERFLOWED\r\n", result=False),
    Case(
        "vop_upsert_bad_value",
        _vop_upsert,
        b"CLIENT_ERROR bad value\r\n",
        error=ArcusClientError,
    ),
    Case(
        "vop_search_results",
        _vop_search,
        b"VECTORS 2\r\na 0.5\r\nb 1.5e-5\r\nEND\r\n",
        result=[("a", 0.5, None), ("b", 1.5e-5, None)],
    ),
    Case("vop_search_empty", _vop_search, b"VECTORS 0\r\nEND\r\n", result=[]),
    Case(
        "vop_search_with_meta",
        _vop_search_with_meta,
        b'VECTORS 1\r\na 0.5 11\r\n{"n":"\xea\xb0\x80"}\r\nEND\r\n',
        result=[("a", 0.5, {"n": "가"})],
    ),
    Case("vop_search_not_found", _vop_search, b"NOT_FOUND\r\n"),
    Case("vop_search_no_element", _vop_search, b"NOT_FOUND_ELEMENT\r\n"),
    Case("vop_search_type_mismatch", _vop_search, b"TYPE_MISMATCH\r\n"),
    Case(
        "vop_search_bad_count",
        _vop_search,
        b"VECTORS x\r\nEND\r\n",
        error=ArcusConnectionError,
    ),
    Case(
        "vop_search_bad_line",
        _vop_search,
        b"VECTORS 1\r\na\r\nEND\r\n",
        error=ArcusConnectionError,
    ),
    Case(
        "vop_search_missing_end",
        _vop_search,
        b"VECTORS 1\r\na 0.5\r\nSTORED\r\n",
        error=ArcusConnectionError,
    ),
    Case(
        "vop_search_bad_meta",
        _vop_search_with_meta,
        b"VECTORS 1\r\na 0.5 3\r\nnot\r\nEND\r\n",
        error=ArcusDecodeError,
    ),
    Case(
        "vop_search_bad_filter",
        _vop_search,
        b"CLIENT_ERROR bad value\r\n",
        error=ArcusClientError,
    ),
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
