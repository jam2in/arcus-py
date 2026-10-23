"""Line and data block boundaries of the memcached text protocol."""

from typing import Final

from arcus._protocol.request import Exchange, ReadExactly
from arcus.exceptions import ArcusConnectionError

CRLF: Final[bytes] = b"\r\n"
MAX_LINE_LENGTH: Final[int] = 65536


def read_data_block(size: int) -> Exchange[bytes]:
    """Read a ``size``-byte data block and its terminating CRLF."""
    block = yield ReadExactly(size + len(CRLF))
    if block[size:] != CRLF:
        raise ArcusConnectionError("Data block is not terminated by CRLF")
    return block[:size]
