"""Encoding of what vector commands carry, and decoding of returned metadata."""

import json
import math
import struct
from collections.abc import Sequence
from typing import Any, Final

from arcus._validation import validate_vkey
from arcus.exceptions import ArcusDecodeError

MAX_FILTER_LENGTH: Final = 4096


def encode_vkey(vkey: str) -> bytes:
    validate_vkey(vkey)
    return vkey.encode()


def encode_vector(vector: Sequence[float] | bytes) -> bytes:
    match vector:
        case bytes() | bytearray() | memoryview():
            data = bytes(vector)
        case Sequence():
            if not math.isfinite(sum(vector)):
                raise ValueError("Vector must hold only finite numbers")
            data = struct.pack(f"<{len(vector)}f", *vector)
        case _:
            raise TypeError(f"Vector must be a sequence, not {type(vector).__name__}")

    if not data:
        raise ValueError("Vector must not be empty")
    if len(data) % 4:
        raise ValueError(f"Vector bytes must be a multiple of 4: {len(data)}")
    return data


def encode_meta(meta: dict[str, Any] | None) -> bytes:
    if meta is None:
        return b""
    if not isinstance(meta, dict):
        raise TypeError(f"Meta must be dict, not {type(meta).__name__}")

    return json.dumps(
        meta, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode()


def decode_meta(vkey: str, data: bytes | None) -> dict[str, Any] | None:
    if data is None:
        return None
    try:
        return json.loads(data)
    except (ValueError, RecursionError) as error:
        raise ArcusDecodeError(f"Failed to decode metadata of {vkey!r}") from error


def encode_filter(filter: str | None) -> bytes:
    if filter is None:
        return b""
    if not isinstance(filter, str):
        raise TypeError(f"Filter must be str, not {type(filter).__name__}")
    filter_bytes = filter.encode()
    if not 1 <= len(filter_bytes) <= MAX_FILTER_LENGTH:
        raise ValueError(
            f"Filter must be 1 to {MAX_FILTER_LENGTH} bytes: {len(filter_bytes)}"
        )

    return filter_bytes
