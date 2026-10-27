"""Key-value command requests and response parsing."""

import re
from typing import Any, Final

from arcus._command._base import SyncCommandExecutor
from arcus._protocol.framing import CRLF, read_data_block
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._protocol.status import raise_unexpected
from arcus.exceptions import ArcusConnectionError, ArcusDecodeError
from arcus.transcoder import ArcusTranscoder

MAX_KEY_LENGTH: Final = 4_000

# The server takes a 4-byte signed exptime; -1 makes a sticky item that
# neither expires nor gets evicted.
STICKY_EXPTIME: Final = -1
MAX_EXPTIME: Final = 2**31 - 1

# Values carry no type information, so every item is stored with flags 0.
_FLAGS: Final = 0

_INVALID_KEY_BYTE: Final = re.compile(rb"[\x00-\x20\x7f]")


def encode_key(key: str) -> bytes:
    if not isinstance(key, str):
        raise TypeError(f"Key must be str, not {type(key).__name__}")
    key_bytes = key.encode()
    if not 1 <= len(key_bytes) <= MAX_KEY_LENGTH:
        raise ValueError(f"Key must be 1 to {MAX_KEY_LENGTH} bytes: {len(key_bytes)}")
    if _INVALID_KEY_BYTE.search(key_bytes):
        raise ValueError(f"Key contains invalid byte: {key_bytes!r}")

    return key_bytes


def validate_exptime(exptime: int) -> int:
    if isinstance(exptime, bool) or not isinstance(exptime, int):
        raise TypeError(f"Exptime must be int, not {type(exptime).__name__}")
    if not STICKY_EXPTIME <= exptime <= MAX_EXPTIME:
        raise ValueError(
            f"Exptime must be {STICKY_EXPTIME} to {MAX_EXPTIME}: {exptime}"
        )

    return exptime


def _parse_value_line(line: bytes, key: bytes) -> int:
    """Parse ``VALUE <key> <flags> <bytes>`` and return the data size."""
    fields = line.split(b" ")
    if (
        len(fields) != 4
        or fields[1] != key
        or not fields[2].isdigit()
        or not fields[3].isdigit()
    ):
        raise ArcusConnectionError(f"Invalid VALUE line: {line!r}")
    return int(fields[3])


def _get(key: bytes, transcoder: ArcusTranscoder) -> Exchange[object | None]:
    request = b"get " + key + CRLF
    yield Write(request)

    line = yield ReadLine()
    if line == b"END":
        return None
    if not line.startswith(b"VALUE "):
        raise_unexpected(line)

    size = _parse_value_line(line, key)
    data = yield from read_data_block(size)

    line = yield ReadLine()
    if line != b"END":
        raise_unexpected(line)

    try:
        return transcoder.decode(data)
    except (ValueError, RecursionError) as error:
        raise ArcusDecodeError(f"Failed to decode value of {key!r}") from error


def prepare_get(
    key: str,
    transcoder: ArcusTranscoder,
) -> Exchange[object | None]:
    return _get(encode_key(key), transcoder)


def _set(key: bytes, exptime: int, data: bytes) -> Exchange[bool]:
    header = b"set %s %d %d %d" % (key, _FLAGS, exptime, len(data))
    request = header + CRLF + data + CRLF
    yield Write(request)

    line = yield ReadLine()
    if line == b"STORED":
        return True
    if line == b"TYPE_MISMATCH":
        return False
    raise_unexpected(line)


def prepare_set(
    key: str,
    value: object,
    exptime: int,
    transcoder: ArcusTranscoder,
) -> Exchange[bool]:
    key_bytes = encode_key(key)
    exptime = validate_exptime(exptime)
    return _set(key_bytes, exptime, transcoder.encode(value))


class KVCommands(SyncCommandExecutor):
    def get(self, key: str) -> Any | None:
        """Return the cached value, or None on a cache miss"""
        return self._execute(encode_key(key), prepare_get(key, self._transcoder))

    def set(self, key: str, value: Any, exptime: int = 0) -> bool:
        """Store the value in the cache and return whether it was stored."""
        exchange = prepare_set(key, value, exptime, self._transcoder)
        return self._execute(encode_key(key), exchange)
