"""Vector command requests and response parsing, and the handle on an index."""

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from arcus._protocol.framing import CRLF
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._protocol.status import raise_unexpected
from arcus._validation import validate_exptime, validate_int

if TYPE_CHECKING:
    from arcus.client import Arcus

DIM_RANGE: Final = range(1, 65_536)
M_RANGE: Final = range(2, 1025)
EF_RANGE: Final = range(1, 65_536)
MAX_COUNT_RANGE: Final = range(2**31)
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


class Vector:
    """A handle on one vector index; get it from ``Arcus.vop_create``."""

    __slots__ = ("_client", "_name")

    def __init__(self, client: "Arcus", name: str) -> None:
        self._client = client
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"Vector({self._name!r})"
