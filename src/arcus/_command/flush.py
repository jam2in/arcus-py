"""Flush command requests and response parsing."""

from collections.abc import Callable
from typing import Final

from arcus._protocol.framing import CRLF
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._protocol.status import raise_unexpected
from arcus._validation import MAX_EXPTIME, validate_int

# The server reads the delay as an exptime: a 64-bit signed integer.
_DELAY_RANGE: Final = range(MAX_EXPTIME + 1)

# The server's name for the keys without a prefix.
_NULL_PREFIX: Final = b"<null>"


def _flush(request: bytes) -> Exchange[bool]:
    yield Write(request)

    line = yield ReadLine()
    if line == b"OK":
        return True
    if line == b"NOT_FOUND":
        return False
    raise_unexpected(line)


def prepare_flush(
    prefix: bytes | None,
    delay: int,
) -> Callable[[], Exchange[bool]]:
    """
    Validate the delay and return a factory of flush exchanges.

    Every node needs its own exchange, because an exchange runs only once.
    """
    validate_int("Delay", delay, _DELAY_RANGE)
    if prefix is None:
        request = b"flush_all %d" % delay + CRLF
    else:
        request = b"flush_prefix %s %d" % (prefix or _NULL_PREFIX, delay) + CRLF
    return lambda: _flush(request)
