"""Argument validation shared by the command modules."""

import re
from typing import Final

MAX_KEY_LENGTH: Final = 4_000

# The server takes a 4-byte signed exptime; -1 makes a sticky item that
# neither expires nor gets evicted.
STICKY_EXPTIME: Final = -1
MAX_EXPTIME: Final = 2**63 - 1

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
