"""Cache values stored as UTF-8 JSON, gzip-compressed when large."""

import json
import zlib
from typing import Final

_COMPRESSION_THRESHOLD: Final = 16 * 1024
_GZIP_WBITS: Final = 16 + zlib.MAX_WBITS
# JSON text never starts with this control byte, so it marks compressed data.
_GZIP_MAGIC: Final = b"\x1f\x8b"


class ArcusTranscoder:
    """Convert cache values to and from UTF-8 JSON, following the ``json`` module.

    None is rejected so that get() can return None for a miss,
    and values larger than 16 KiB are gzip-compressed.
    """

    def encode(self, value: object) -> bytes:
        if value is None:
            raise TypeError("None cannot be stored; get() returns None on a miss")
        data = json.dumps(value, ensure_ascii=False).encode()
        if len(data) > _COMPRESSION_THRESHOLD:
            compressed = zlib.compress(data, wbits=_GZIP_WBITS)
            if len(compressed) < len(data):
                return compressed
        return data

    def decode(self, data: bytes) -> object:
        if data.startswith(_GZIP_MAGIC):
            try:
                data = zlib.decompress(data, _GZIP_WBITS)
            except zlib.error as error:
                raise ValueError("Invalid gzip-compressed value") from error
        return json.loads(data)
