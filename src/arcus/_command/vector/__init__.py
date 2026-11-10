"""Vector command requests and response parsing, and the handle on an index."""

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

from arcus._command.vector._transcoder import (
    decode_meta,
    encode_filter,
    encode_meta,
    encode_vector,
    encode_vkey,
)
from arcus._protocol.framing import CRLF, read_data_block
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._protocol.status import raise_unexpected
from arcus._validation import validate_exptime, validate_int
from arcus.exceptions import ArcusConnectionError

if TYPE_CHECKING:
    from arcus.client import Arcus

DIM_RANGE: Final = range(1, 65_536)
M_RANGE: Final = range(2, 1025)
EF_RANGE: Final = range(1, 65_536)
MAX_COUNT_RANGE: Final = range(2**31)
COUNT_RANGE: Final = range(1, 2**31)
DEFAULT_EF_CONSTRUCTION: Final = 128
DEFAULT_EF_SEARCH: Final = 64


class Metric(StrEnum):
    """The distance a vector index ranks its elements by."""

    COSINE = "cos"
    L2 = "l2"
    IP = "ip"
    HAMMING = "hamming"
    TANIMOTO = "tanimoto"


class Quant(StrEnum):
    """The scalar type a vector index stores each dimension as."""

    F32 = "f32"
    F16 = "f16"
    I8 = "i8"
    B1 = "b1"


_BITWISE_METRICS: Final = frozenset({Metric.HAMMING, Metric.TANIMOTO})


@dataclass(frozen=True, slots=True, kw_only=True)
class VectorOptions:
    """The options of a vector index, given to ``Arcus.vop_create``."""

    dim: int
    metric: Metric = Metric.COSINE
    quant: Quant = Quant.F32
    m: int = 16
    ef_construction: int = DEFAULT_EF_CONSTRUCTION
    ef_search: int = DEFAULT_EF_SEARCH
    exptime: int = 0
    max_count: int = 0

    def __post_init__(self) -> None:
        validate_int("Dim", self.dim, DIM_RANGE)
        validate_int("M", self.m, M_RANGE)
        validate_int("Ef_construction", self.ef_construction, EF_RANGE)
        validate_int("Ef_search", self.ef_search, EF_RANGE)
        validate_exptime(self.exptime)
        validate_int("Max_count", self.max_count, MAX_COUNT_RANGE)
        if not isinstance(self.metric, Metric):
            raise TypeError(f"Metric must be Metric, not {type(self.metric).__name__}")
        if not isinstance(self.quant, Quant):
            raise TypeError(f"Quant must be Quant, not {type(self.quant).__name__}")
        if (self.metric in _BITWISE_METRICS) != (self.quant is Quant.B1):
            raise ValueError(
                f"Metric {self.metric} cannot be used with quant {self.quant}"
            )


def _create(index: bytes, options: VectorOptions) -> Exchange[bool]:
    request = (
        b"vop create %s %d %d %d metric %s quant %s M %d efc %d efs %d"
        % (
            index,
            options.dim,
            options.exptime,
            options.max_count,
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


def prepare_vop_create(index: bytes, options: VectorOptions) -> Exchange[bool]:
    """Check the options are VectorOptions, which validate themselves."""
    if not isinstance(options, VectorOptions):
        raise TypeError(f"Options must be VectorOptions, not {type(options).__name__}")
    return _create(index, options)


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


def prepare_vop_upsert(
    index: bytes,
    vkey: str,
    vector: Sequence[float] | bytes,
    meta: dict[str, Any] | None,
) -> Exchange[bool]:
    """Validate and encode the element before the exchange starts."""
    key_bytes = encode_vkey(vkey)
    vector_bytes = encode_vector(vector)
    meta_bytes = encode_meta(meta)
    return _upsert(index, key_bytes, vector_bytes, meta_bytes)


SearchResult = tuple[str, float, dict[str, Any] | None]


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
    count: int,
    filter: bytes,
    with_meta: bool,
) -> Exchange[list[SearchResult] | None]:
    header = b"vop search %s vkey %s %d" % (index, vkey, count)
    header += _search_options(filter, with_meta)
    request = header + CRLF
    if filter:
        request += filter + CRLF
    return _search(request, with_meta)


def _search_by_vector(
    index: bytes,
    vector: bytes,
    count: int,
    filter: bytes,
    with_meta: bool,
) -> Exchange[list[SearchResult] | None]:
    header = b"vop search %s vector %d %d" % (index, len(vector), count)
    header += _search_options(filter, with_meta)
    request = header + CRLF + vector + CRLF
    if filter:
        request += filter + CRLF
    return _search(request, with_meta)


def _parse_vectors_line(line: bytes) -> int:
    """Parse ``VECTORS <count>`` and return the count."""
    fields = line.split(b" ")
    if len(fields) != 2 or not fields[1].isdigit():
        raise ArcusConnectionError(f"Invalid VECTORS line: {line!r}")
    return int(fields[1])


def _parse_result_line(line: bytes, with_meta: bool) -> tuple[str, float, int]:
    """Parse ``<vkey> <score> [<meta bytes>]`` into the vkey, score and meta size."""
    fields = line.split(b" ")
    try:
        if with_meta:
            key, score, size = fields
            return key.decode(), float(score), int(size)
        else:
            key, score = fields
            return key.decode(), float(score), 0
    except ValueError as error:
        raise ArcusConnectionError(f"Invalid search result line: {line!r}") from error


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
        if with_meta:
            data = yield from read_data_block(size)
            rows.append((vkey, score, data))
        else:
            rows.append((vkey, score, None))

    line = yield ReadLine()
    if line != b"END":
        raise_unexpected(line)

    return [(vkey, score, decode_meta(vkey, data)) for vkey, score, data in rows]


def prepare_vop_search(
    index: bytes,
    query: Sequence[float] | bytes | str,
    count: int,
    filter: str | None,
    with_meta: bool,
) -> Exchange[list[SearchResult] | None]:
    """Validate and encode the search before the exchange starts."""
    validate_int("Count", count, COUNT_RANGE)
    filter_bytes = encode_filter(filter)

    if isinstance(query, str):
        vkey_bytes = encode_vkey(query)
        return _search_by_vkey(index, vkey_bytes, count, filter_bytes, with_meta)
    else:
        vector_bytes = encode_vector(query)
        return _search_by_vector(index, vector_bytes, count, filter_bytes, with_meta)


class Vector:
    """A handle on one vector index; get it from ``Arcus.vop_create``."""

    __slots__ = ("_client", "_name")

    def __init__(self, client: "Arcus", name: str) -> None:
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
        query: Sequence[float] | bytes | str,
        count: int,
        *,
        filter: str | None = None,
        with_meta: bool = False,
    ) -> list[SearchResult] | None:
        return self._client.vop_search(
            self._name, query, count, filter=filter, with_meta=with_meta
        )

    def __repr__(self) -> str:
        return f"Vector({self._name!r})"
