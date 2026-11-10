"""Encoding of the vkey, vector and metadata that vector commands carry."""

import json
import math
import struct
from collections.abc import Sequence
from typing import Any

from arcus._validation import validate_vkey


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
