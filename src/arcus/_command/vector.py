"""Vector command requests and response parsing, and the handle on an index."""

import json
import math
import struct
from collections.abc import Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, Generic, TypeVar

from arcus._command import encode_key, validate_exptime
from arcus._protocol.framing import CRLF, read_data_block
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._protocol.status import raise_unexpected
from arcus.exceptions import ArcusConnectionError, ArcusDecodeError

if TYPE_CHECKING:
    from arcus.client import Arcus

MAX_DIM: Final = 65_535
MAX_MAXCOUNT: Final = 2**32 - 1
M_RANGE: Final = range(2, 1025)
EF_RANGE: Final = range(1, 65_537)
MAX_VKEY_LENGTH: Final = 250
MAX_RESULTS: Final = 2**31 - 1
MAX_FILTER_LENGTH: Final = 4096


class Metric(StrEnum):
    COSINE = "cos"
    L2 = "l2"
    IP = "ip"
    HAMMING = "hamming"
    TANIMOTO = "tanimoto"


class Quantization(StrEnum):
    F32 = "f32"
    F16 = "f16"
    I8 = "i8"
    B1 = "b1"


_BITWISE_METRICS: Final = frozenset({Metric.HAMMING, Metric.TANIMOTO})


def _validate_int(name: str, value: int, valid: range) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int, not {type(value).__name__}")
    if value not in valid:
        raise ValueError(f"{name} must be {valid.start} to {valid.stop - 1}: {value}")

    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class VopCreateOptions:
    """The options of ``vop create``; the client fills each from its arguments."""

    exptime: int
    maxcount: int
    metric: str
    quant: str
    m: int
    ef_construction: int
    ef_search: int


def _create(index: bytes, dim: int, options: VopCreateOptions) -> Exchange[bool]:
    request = (
        b"vop create %s %d %d %d metric %s quant %s M %d efc %d efs %d"
        % (
            index,
            dim,
            options.exptime,
            options.maxcount,
            options.metric.encode(),
            options.quant.encode(),
            options.m,
            options.ef_construction,
            options.ef_search,
        )
        + CRLF
    )
    yield Write(request)

    line = yield ReadLine()
    if line == b"CREATED":
        return True
    if line == b"EXISTS":
        return False
    raise_unexpected(line)


def prepare_vop_create(
    index: bytes,
    dim: int,
    options: VopCreateOptions,
) -> Exchange[bool]:
    """Validate the arguments before the exchange starts."""
    dim = _validate_int("Dim", dim, range(1, MAX_DIM + 1))
    options = replace(
        options,
        exptime=validate_exptime(options.exptime),
        maxcount=_validate_int("Maxcount", options.maxcount, range(MAX_MAXCOUNT + 1)),
        metric=Metric(options.metric),
        quant=Quantization(options.quant),
        m=_validate_int("M", options.m, M_RANGE),
        ef_construction=_validate_int(
            "Ef_construction", options.ef_construction, EF_RANGE
        ),
        ef_search=_validate_int("Ef_search", options.ef_search, EF_RANGE),
    )
    if (options.metric in _BITWISE_METRICS) != (options.quant is Quantization.B1):
        raise ValueError(
            f"Metric {options.metric} cannot be used with quant {options.quant}"
        )
    return _create(index, dim, options)


def _upsert(
    index: bytes,
    vkey: bytes,
    vector: bytes,
    meta: bytes,
) -> Exchange[bool]:
    header = b"vop upsert %s %s %d" % (index, vkey, len(vector))
    if meta:
        header += b" meta %d" % len(meta)
        request = header + CRLF + vector + CRLF + meta + CRLF
    else:
        request = header + CRLF + vector + CRLF
    yield Write(request)

    line = yield ReadLine()
    if line in (b"STORED", b"REPLACED"):
        return True
    if line in (b"NOT_FOUND", b"TYPE_MISMATCH", b"OVERFLOWED"):
        return False
    raise_unexpected(line)


def _encode_vkey(vkey: str) -> bytes:
    key_bytes = encode_key(vkey)
    if len(key_bytes) > MAX_VKEY_LENGTH:
        raise ValueError(f"Vkey must be 1 to {MAX_VKEY_LENGTH} bytes: {len(key_bytes)}")

    return key_bytes


def _encode_vector(vector: Sequence[float] | bytes) -> bytes:
    match vector:
        case bytes() | bytearray() | memoryview():
            data = bytes(vector)
        case Sequence():
            if not math.isfinite(sum(vector)):
                raise ValueError("Vector must hold only finite numbers")
            data = struct.pack(f"<{len(vector)}f", *vector)
        case _:
            raise TypeError(f"Vector must be a sequence, not {type(vector).__name__}")

    if not data:
        raise ValueError("Vector must not be empty")
    if len(data) % 4:
        raise ValueError(f"Vector bytes must be a multiple of 4: {len(data)}")
    return data


def _encode_meta(meta: dict[str, Any] | None) -> bytes:
    if meta is None:
        return b""
    if not isinstance(meta, dict):
        raise TypeError(f"Meta must be dict, not {type(meta).__name__}")

    return json.dumps(
        meta, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode()


def prepare_vop_upsert(
    index: bytes,
    vkey: str,
    vector: Sequence[float] | bytes,
    meta: dict[str, Any] | None,
) -> Exchange[bool]:
    """Validate and encode the element before the exchange starts."""
    key_bytes = _encode_vkey(vkey)
    vector_bytes = _encode_vector(vector)
    meta_bytes = _encode_meta(meta)
    return _upsert(index, key_bytes, vector_bytes, meta_bytes)


# A search result: the vkey, its score, and its metadata if asked for.
SearchResult = tuple[str, float, dict[str, Any] | None]


def _parse_result_line(line: bytes, with_meta: bool) -> tuple[str, float, int]:
    """Parse ``<vkey> <score> [<meta bytes>]`` into the vkey, score and meta size."""
    fields = line.split(b" ")
    try:
        if with_meta:
            key, score, size = fields
            return key.decode(), float(score), int(size)
        key, score = fields
        return key.decode(), float(score), 0
    except ValueError as error:
        raise ArcusConnectionError(f"Invalid search result line: {line!r}") from error


def _search_options(filter: bytes, with_meta: bool) -> bytes:
    options = b""
    if filter:
        options += b" filter %d" % len(filter)
    if with_meta:
        options += b" withmeta"
    return options


def _search_by_vkey(
    index: bytes,
    vkey: bytes,
    num: int,
    filter: bytes,
    with_meta: bool,
) -> Exchange[list[SearchResult] | None]:
    header = b"vop search %s vkey %s %d" % (index, vkey, num)
    header += _search_options(filter, with_meta)
    if filter:
        request = header + CRLF + filter + CRLF
    else:
        request = header + CRLF
    return _search(request, with_meta)


def _search_by_vector(
    index: bytes,
    vector: bytes,
    num: int,
    filter: bytes,
    with_meta: bool,
) -> Exchange[list[SearchResult] | None]:
    header = b"vop search %s vector %d %d" % (index, len(vector), num)
    header += _search_options(filter, with_meta)
    if filter:
        request = header + CRLF + vector + CRLF + filter + CRLF
    else:
        request = header + CRLF + vector + CRLF
    return _search(request, with_meta)


def _parse_vectors_line(line: bytes) -> int:
    """Parse ``VECTORS <count>`` and return the count."""
    fields = line.split(b" ")
    if len(fields) != 2 or not fields[1].isdigit():
        raise ArcusConnectionError(f"Invalid VECTORS line: {line!r}")
    return int(fields[1])


def _search(request: bytes, with_meta: bool) -> Exchange[list[SearchResult] | None]:
    yield Write(request)

    line = yield ReadLine()
    if line in (b"NOT_FOUND", b"NOT_FOUND_ELEMENT", b"TYPE_MISMATCH"):
        return None
    if not line.startswith(b"VECTORS "):
        raise_unexpected(line)

    count = _parse_vectors_line(line)
    rows: list[tuple[str, float, bytes | None]] = []
    for _ in range(count):
        line = yield ReadLine()
        vkey, score, size = _parse_result_line(line, with_meta)
        data = (yield from read_data_block(size)) if with_meta else None
        rows.append((vkey, score, data))

    line = yield ReadLine()
    if line != b"END":
        raise_unexpected(line)

    return [(vkey, score, _decode_meta(vkey, data)) for vkey, score, data in rows]


def _decode_meta(vkey: str, data: bytes | None) -> dict[str, Any] | None:
    if data is None:
        return None
    try:
        return json.loads(data)
    except (ValueError, RecursionError) as error:
        raise ArcusDecodeError(f"Failed to decode metadata of {vkey!r}") from error


def _encode_filter(filter: str | None) -> bytes:
    if filter is None:
        return b""
    if not isinstance(filter, str):
        raise TypeError(f"Filter must be str, not {type(filter).__name__}")
    filter_bytes = filter.encode()
    if not 1 <= len(filter_bytes) <= MAX_FILTER_LENGTH:
        raise ValueError(
            f"Filter must be 1 to {MAX_FILTER_LENGTH} bytes: {len(filter_bytes)}"
        )

    return filter_bytes


def prepare_vop_search(
    index: bytes,
    vector: Sequence[float] | bytes | str,
    num: int,
    filter: str | None,
    with_meta: bool,
) -> Exchange[list[SearchResult] | None]:
    """Validate and encode the search before the exchange starts."""
    num = _validate_int("Num", num, range(1, MAX_RESULTS + 1))
    filter_bytes = _encode_filter(filter)

    if isinstance(vector, str):
        vkey_bytes = _encode_vkey(vector)
        return _search_by_vkey(index, vkey_bytes, num, filter_bytes, with_meta)
    else:
        vector_bytes = _encode_vector(vector)
        return _search_by_vector(index, vector_bytes, num, filter_bytes, with_meta)


# The client a handle runs its commands on; an async client joins it later.
C = TypeVar("C", bound="Arcus")


class Vector(Generic[C]):
    """A handle on one vector index; get it from ``Arcus.vop_create``."""

    __slots__ = ("_client", "_name")

    def __init__(self, client: C, name: str) -> None:
        self._client = client
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def upsert(
        self,
        vkey: str,
        vector: Sequence[float] | bytes,
        *,
        meta: dict[str, Any] | None = None,
    ) -> bool:
        return self._client.vop_upsert(self._name, vkey, vector, meta=meta)

    def search(
        self,
        vector: Sequence[float] | bytes | str,
        num: int,
        *,
        filter: str | None = None,
        with_meta: bool = False,
    ) -> list[SearchResult] | None:
        return self._client.vop_search(
            self._name, vector, num, filter=filter, with_meta=with_meta
        )

    def __repr__(self) -> str:
        return f"Vector({self._name!r})"
