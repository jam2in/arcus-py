"""Key-value command requests and response parsing."""

from typing import Final

from arcus._protocol.framing import CRLF, read_data_block
from arcus._protocol.request import Exchange, ReadLine, Write
from arcus._protocol.status import raise_unexpected
from arcus._validation import validate_exptime
from arcus.exceptions import ArcusConnectionError, ArcusDecodeError
from arcus.transcoder import ArcusTranscoder

# Values carry no type information, so every item is stored with flags 0.
_FLAGS: Final = 0


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
    key: bytes,
    transcoder: ArcusTranscoder,
) -> Exchange[object | None]:
    return _get(key, transcoder)


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
    key: bytes,
    value: object,
    exptime: int,
    transcoder: ArcusTranscoder,
) -> Exchange[bool]:
    """Validate the exptime and encode the value before the exchange starts."""
    validate_exptime(exptime)
    return _set(key, exptime, transcoder.encode(value))
