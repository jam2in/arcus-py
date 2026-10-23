"""Key-value command requests and response parsing."""

from typing import Final

from arcus._protocol.framing import CRLF, read_data_block
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._protocol.status import raise_unexpected
from arcus.exceptions import ArcusConnectionError, ArcusDecodeError
from arcus.transcoder import ArcusTranscoder

MAX_KEY_LENGTH: Final = 4000

# Values carry no type information, so every item is stored with flags 0.
_FLAGS: Final = 0

# Complete replies that store nothing; the Java client returns false for them.
_NOT_STORED: Final = frozenset(
    {b"NOT_STORED", b"NOT_FOUND", b"EXISTS", b"TYPE_MISMATCH"}
)


def encode_key(key: str) -> bytes:
    if not isinstance(key, str):
        raise TypeError(f"Key must be str, not {type(key).__name__}")
    key_bytes = key.encode()
    if not 1 <= len(key_bytes) <= MAX_KEY_LENGTH:
        raise ValueError(f"Key must be 1 to {MAX_KEY_LENGTH} bytes: {len(key_bytes)}")
    if any(byte <= 0x20 or byte == 0x7F for byte in key_bytes):
        raise ValueError(f"Key contains whitespace or control characters: {key!r}")
    return key_bytes


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
    yield Write(b"get " + key + CRLF)

    line = yield ReadLine()
    if line in (b"END", b"TYPE_MISMATCH"):
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


def _store(request: bytes) -> Exchange[bool]:
    yield Write(request)

    line = yield ReadLine()
    if line == b"STORED":
        return True
    if line in _NOT_STORED:
        return False
    raise_unexpected(line)


def _prepare_store(
    command: bytes,
    key: str,
    value: object,
    exptime: int,
    transcoder: ArcusTranscoder,
) -> Exchange[bool]:
    """Build a storage command; add, replace, append and prepend reuse it."""
    key_bytes = encode_key(key)
    if isinstance(exptime, bool) or not isinstance(exptime, int):
        raise TypeError(f"Expiration time must be int, not {type(exptime).__name__}")
    data = transcoder.encode(value)
    header = b"%s %s %d %d %d" % (command, key_bytes, _FLAGS, exptime, len(data))
    return _store(header + CRLF + data + CRLF)


def prepare_get(key: str, transcoder: ArcusTranscoder) -> Exchange[object | None]:
    return _get(encode_key(key), transcoder)


def prepare_set(
    key: str,
    value: object,
    exptime: int,
    transcoder: ArcusTranscoder,
) -> Exchange[bool]:
    return _prepare_store(b"set", key, value, exptime, transcoder)
