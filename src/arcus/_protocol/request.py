"""I/O-independent request exchanges driven by the sync and async connections."""

from collections.abc import Generator
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Write:
    """Send all bytes; the driver resumes the exchange with ``b""``."""

    data: bytes


@dataclass(frozen=True, slots=True)
class ReadLine:
    """Read one line; the driver resumes the exchange with it, without CRLF."""


@dataclass(frozen=True, slots=True)
class ReadExactly:
    """Read exactly ``size`` bytes."""

    size: int


Op = Write | ReadLine | ReadExactly
Exchange = Generator[Op, bytes, T]
