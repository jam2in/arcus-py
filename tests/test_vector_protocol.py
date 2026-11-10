import math
import struct
from collections.abc import Sequence
from typing import Any

import pytest
from support.drive import drive

from arcus._command.vector import (
    EF_RANGE,
    M_RANGE,
    MAX_DIM,
    MAX_FILTER_LENGTH,
    MAX_RESULTS,
    MAX_VKEY_LENGTH,
    Metric,
    Quantization,
    VopCreateOptions,
    prepare_vop_create,
    prepare_vop_search,
    prepare_vop_upsert,
)

DEFAULTS: dict[str, Any] = {
    "exptime": 0,
    "maxcount": 0,
    "metric": Metric.COSINE,
    "quant": Quantization.F32,
    "m": 16,
    "ef_construction": 128,
    "ef_search": 64,
}


def _options(**options: Any) -> VopCreateOptions:
    return VopCreateOptions(**{**DEFAULTS, **options})


def _create(dim: int = 3, **options: Any) -> bytes:
    exchange = prepare_vop_create(b"idx", dim, _options(**options))
    _, sent = drive(exchange, b"CREATED\r\n")
    return sent


def test_create_request_bytes() -> None:
    # Given / When: A create exchange with the default options is driven.
    sent = _create()

    # Then: Every option is sent, with maxcount 0 for the server default.
    assert sent == b"vop create idx 3 0 0 metric cos quant f32 M 16 efc 128 efs 64\r\n"


def test_create_sends_maxcount_as_the_fourth_argument() -> None:
    # Given / When: A create exchange with a maxcount and an exptime is driven.
    sent = _create(exptime=60, maxcount=100)

    # Then: The maxcount follows the exptime as the positional fourth argument.
    assert sent.startswith(b"vop create idx 3 60 100 metric ")


def test_create_sends_every_option() -> None:
    # Given / When: A create exchange with every option changed is driven.
    sent = _create(
        8,
        metric=Metric.L2,
        quant=Quantization.F16,
        m=8,
        ef_construction=64,
        ef_search=32,
    )

    # Then: Each option is sent with its protocol keyword.
    assert sent == b"vop create idx 8 0 0 metric l2 quant f16 M 8 efc 64 efs 32\r\n"


def test_create_accepts_metric_and_quant_as_str() -> None:
    # Given / When: The metric and quant are given as their protocol words.
    sent = _create(metric="hamming", quant="b1")

    # Then: They are sent as the same words.
    assert b" metric hamming quant b1 " in sent


@pytest.mark.parametrize(
    ("dim", "options"),
    [
        pytest.param(1, {}, id="min-dim"),
        pytest.param(MAX_DIM, {}, id="max-dim"),
        pytest.param(3, {"m": M_RANGE.start}, id="min-m"),
        pytest.param(3, {"m": M_RANGE.stop - 1}, id="max-m"),
        pytest.param(3, {"ef_search": EF_RANGE.stop - 1}, id="max-ef"),
        pytest.param(3, {"maxcount": 2**32 - 1}, id="max-maxcount"),
    ],
)
def test_create_bounds_are_accepted(dim: int, options: dict[str, Any]) -> None:
    # Given / When: A create exchange at either end of a range is driven.
    sent = _create(dim, **options)

    # Then: The command is sent.
    assert sent.startswith(b"vop create idx ")


@pytest.mark.parametrize(
    ("dim", "options", "error"),
    [
        pytest.param(0, {}, ValueError, id="zero-dim"),
        pytest.param(MAX_DIM + 1, {}, ValueError, id="too-large-dim"),
        pytest.param(True, {}, TypeError, id="bool-dim"),
        pytest.param(3, {"m": M_RANGE.start - 1}, ValueError, id="small-m"),
        pytest.param(3, {"ef_construction": 0}, ValueError, id="zero-efc"),
        pytest.param(3, {"ef_search": 1.5}, TypeError, id="float-efs"),
        pytest.param(3, {"maxcount": -1}, ValueError, id="negative-maxcount"),
        pytest.param(3, {"maxcount": 2**32}, ValueError, id="too-large-maxcount"),
        pytest.param(3, {"metric": "dot"}, ValueError, id="unknown-metric"),
        pytest.param(3, {"quant": "f64"}, ValueError, id="unknown-quant"),
        pytest.param(3, {"metric": Metric.HAMMING}, ValueError, id="hamming-f32"),
        pytest.param(3, {"quant": Quantization.B1}, ValueError, id="cos-b1"),
    ],
)
def test_invalid_create_is_rejected_before_iteration(
    dim: int,
    options: dict[str, Any],
    error: type[Exception],
) -> None:
    # Given: Create arguments the server cannot accept.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(error):
        prepare_vop_create(b"idx", dim, _options(**options))


def _upsert(
    key: str = "v",
    vector: Sequence[float] | bytes = (1.0, -2.5),
    meta: dict[str, Any] | None = None,
) -> bytes:
    exchange = prepare_vop_upsert(b"idx", key, vector, meta)
    _, sent = drive(exchange, b"STORED\r\n")
    return sent


def test_upsert_request_bytes() -> None:
    # Given / When: An upsert exchange without metadata is driven.
    sent = _upsert()

    # Then: The vector is sent as little-endian f32s in a data block.
    vector = struct.pack("<2f", 1.0, -2.5)
    assert sent == b"vop upsert idx v 8\r\n" + vector + b"\r\n"


def test_upsert_sends_meta_after_the_vector() -> None:
    # Given / When: An upsert exchange with metadata is driven.
    sent = _upsert(meta={"z": 1, "가": "값"})

    # Then: The compact JSON, in key order and UTF-8, follows the vector.
    meta = '{"z":1,"가":"값"}'.encode()
    vector = struct.pack("<2f", 1.0, -2.5)
    assert sent == (
        b"vop upsert idx v 8 meta %d\r\n" % len(meta)
        + vector
        + b"\r\n"
        + meta
        + b"\r\n"
    )


def test_upsert_without_meta_sends_none() -> None:
    # Given / When: An upsert exchange without metadata is driven.
    sent = _upsert(meta=None)

    # Then: Only the vector is sent.
    assert sent == b"vop upsert idx v 8\r\n" + struct.pack("<2f", 1.0, -2.5) + b"\r\n"


def test_upsert_sends_an_empty_meta_object() -> None:
    # Given / When: An upsert exchange with empty metadata is driven.
    sent = _upsert(meta={})

    # Then: The empty object is sent after the vector.
    assert sent.startswith(b"vop upsert idx v 8 meta 2\r\n")
    assert sent.endswith(b"\r\n{}\r\n")


@pytest.mark.parametrize(
    "vector",
    [
        pytest.param(struct.pack("<2f", 1.0, -2.5), id="bytes"),
        pytest.param(bytearray(struct.pack("<2f", 1.0, -2.5)), id="bytearray"),
        pytest.param(memoryview(struct.pack("<2f", 1.0, -2.5)), id="memoryview"),
    ],
)
def test_upsert_sends_vector_bytes_as_they_are(vector: bytes) -> None:
    # Given / When: An upsert exchange with the vector already as f32 bytes.
    sent = _upsert(vector=vector)

    # Then: The bytes are sent unchanged, as a sequence of the same floats would be.
    assert sent == _upsert(vector=[1.0, -2.5])


def test_max_length_vkey_is_accepted() -> None:
    # Given / When: An upsert with a vkey of exactly the maximum length is driven.
    sent = _upsert(key="k" * MAX_VKEY_LENGTH)

    # Then: The full vkey is sent.
    assert sent.startswith(b"vop upsert idx " + b"k" * MAX_VKEY_LENGTH + b" 8\r\n")


@pytest.mark.parametrize(
    ("key", "vector", "meta", "error"),
    [
        pytest.param(
            "k" * (MAX_VKEY_LENGTH + 1), [1.0], {}, ValueError, id="long-vkey"
        ),
        pytest.param("a b", [1.0], {}, ValueError, id="space-vkey"),
        pytest.param("v", [], {}, ValueError, id="empty-vector"),
        pytest.param("v", b"", {}, ValueError, id="empty-bytes"),
        pytest.param("v", b"\x00" * 5, {}, ValueError, id="partial-f32-bytes"),
        pytest.param("v", "1.0", {}, TypeError, id="str-vector"),
        pytest.param("v", [math.nan], {}, ValueError, id="nan"),
        pytest.param("v", [math.inf], {}, ValueError, id="inf"),
        pytest.param("v", ["1"], {}, TypeError, id="str-element"),
        pytest.param("v", [1e39], {}, OverflowError, id="past-f32"),
        pytest.param("v", [1.0], [("a", 1)], TypeError, id="list-meta"),
        pytest.param("v", [1.0], {"a": math.nan}, ValueError, id="nan-meta"),
        pytest.param("v", [1.0], {"a": object()}, TypeError, id="unserializable-meta"),
    ],
)
def test_invalid_upsert_is_rejected_before_iteration(
    key: str,
    vector: Any,
    meta: Any,
    error: type[Exception],
) -> None:
    # Given: An element the server cannot accept.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(error):
        prepare_vop_upsert(b"idx", key, vector, meta)


def _search(
    vector: Sequence[float] | bytes | str,
    num: int = 5,
    filter: str | None = None,
    with_meta: bool = False,
) -> bytes:
    exchange = prepare_vop_search(b"idx", vector, num, filter, with_meta)
    _, sent = drive(exchange, b"VECTORS 0\r\nEND\r\n")
    return sent


def test_search_by_vkey_request_bytes() -> None:
    # Given / When: A search by a vkey is driven.
    sent = _search("v")

    # Then: The vkey is sent on the command line, with no body.
    assert sent == b"vop search idx vkey v 5\r\n"


def test_search_by_vector_request_bytes() -> None:
    # Given / When: A search by a vector is driven.
    sent = _search([1.0, -2.5])

    # Then: The vector is sent as little-endian f32s in a data block.
    vector = struct.pack("<2f", 1.0, -2.5)
    assert sent == b"vop search idx vector 8 5\r\n" + vector + b"\r\n"


def test_search_by_vector_bytes_request_bytes() -> None:
    # Given / When: A search by a vector already as f32 bytes is driven.
    vector = struct.pack("<2f", 1.0, -2.5)
    sent = _search(vector)

    # Then: The bytes are sent as the vector, not as a vkey.
    assert sent == b"vop search idx vector 8 5\r\n" + vector + b"\r\n"


def test_search_sends_the_filter_after_the_vector() -> None:
    # Given / When: A search by a vector with a filter and metadata is driven.
    sent = _search((1.0, -2.5), filter="cat = '가'", with_meta=True)

    # Then: The UTF-8 filter follows the vector, and withmeta ends the line.
    filter = "cat = '가'".encode()
    vector = struct.pack("<2f", 1.0, -2.5)
    assert sent == (
        b"vop search idx vector 8 5 filter %d withmeta\r\n" % len(filter)
        + vector
        + b"\r\n"
        + filter
        + b"\r\n"
    )


def test_search_by_vkey_sends_only_the_filter() -> None:
    # Given / When: A search by a vkey with a filter is driven.
    sent = _search("v", filter="n > 1")

    # Then: The filter is the whole body.
    assert sent == b"vop search idx vkey v 5 filter 5\r\nn > 1\r\n"


@pytest.mark.parametrize(
    ("vector", "num", "filter", "error"),
    [
        pytest.param("a b", 5, None, ValueError, id="space-vkey"),
        pytest.param([], 5, None, ValueError, id="empty-vector"),
        pytest.param(b"\x00\x00\x80", 5, None, ValueError, id="partial-f32-bytes"),
        pytest.param([math.nan], 5, None, ValueError, id="nan"),
        pytest.param("v", 0, None, ValueError, id="zero-num"),
        pytest.param("v", MAX_RESULTS + 1, None, ValueError, id="too-large-num"),
        pytest.param("v", True, None, TypeError, id="bool-num"),
        pytest.param("v", 5, "", ValueError, id="empty-filter"),
        pytest.param(
            "v", 5, "a" * (MAX_FILTER_LENGTH + 1), ValueError, id="long-filter"
        ),
        pytest.param("v", 5, b"n = 1", TypeError, id="bytes-filter"),
    ],
)
def test_invalid_search_is_rejected_before_iteration(
    vector: Any,
    num: int,
    filter: Any,
    error: type[Exception],
) -> None:
    # Given: A search the server cannot accept.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(error):
        prepare_vop_search(b"idx", vector, num, filter, False)
