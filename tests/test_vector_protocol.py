import math
import struct
from collections.abc import Sequence
from typing import Any

import pytest
from support.drive import drive

from arcus._command.vector import (
    COUNT_RANGE,
    DIM_RANGE,
    EF_RANGE,
    M_RANGE,
    Metric,
    Quant,
    VectorOptions,
    prepare_vop_create,
    prepare_vop_search,
    prepare_vop_upsert,
)
from arcus._command.vector._transcoder import MAX_FILTER_LENGTH
from arcus._validation import MAX_VKEY_LENGTH


def _create(dim: int = 3, **options: Any) -> bytes:
    exchange = prepare_vop_create(b"idx", VectorOptions(dim=dim, **options))
    _, sent = drive(exchange, b"CREATED\r\n")
    return sent


def test_create_request_bytes() -> None:
    # Given / When: A create exchange with the default options is driven.
    sent = _create()

    # Then: Every option is sent, with max_count 0 for the server default.
    assert sent == b"vop create idx 3 0 0 metric cos quant f32 M 16 efc 128 efs 64\r\n"


def test_create_sends_max_count_as_the_fourth_argument() -> None:
    # Given / When: A create exchange with a max_count and an exptime is driven.
    sent = _create(exptime=60, max_count=100)

    # Then: The max_count follows the exptime as the maxcount argument.
    assert sent.startswith(b"vop create idx 3 60 100 metric ")


def test_create_sends_every_option() -> None:
    # Given / When: A create exchange with every option changed is driven.
    sent = _create(
        8,
        metric=Metric.L2,
        quant=Quant.F16,
        m=8,
        ef_construction=64,
        ef_search=32,
    )

    # Then: Each option is sent with its protocol keyword.
    assert sent == b"vop create idx 8 0 0 metric l2 quant f16 M 8 efc 64 efs 32\r\n"


def test_create_sends_bitwise_metric_with_b1() -> None:
    # Given / When: A bitwise metric is given with the b1 quant.
    sent = _create(metric=Metric.HAMMING, quant=Quant.B1)

    # Then: They are sent as their protocol words.
    assert b" metric hamming quant b1 " in sent


@pytest.mark.parametrize(
    ("dim", "options"),
    [
        pytest.param(1, {}, id="min-dim"),
        pytest.param(DIM_RANGE.stop - 1, {}, id="max-dim"),
        pytest.param(3, {"m": M_RANGE.start}, id="min-m"),
        pytest.param(3, {"m": M_RANGE.stop - 1}, id="max-m"),
        pytest.param(3, {"ef_search": EF_RANGE.stop - 1}, id="max-ef"),
        pytest.param(3, {"max_count": 2**31 - 1}, id="largest-max-count"),
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
        pytest.param(DIM_RANGE.stop, {}, ValueError, id="too-large-dim"),
        pytest.param(True, {}, TypeError, id="bool-dim"),
        pytest.param(3, {"m": M_RANGE.start - 1}, ValueError, id="small-m"),
        pytest.param(3, {"ef_construction": 0}, ValueError, id="zero-efc"),
        pytest.param(3, {"ef_search": EF_RANGE.stop}, ValueError, id="too-large-efs"),
        pytest.param(3, {"ef_search": 1.5}, TypeError, id="float-efs"),
        pytest.param(3, {"max_count": -1}, ValueError, id="negative-max-count"),
        pytest.param(3, {"max_count": 2**31}, ValueError, id="too-large-max-count"),
        pytest.param(3, {"metric": "cos"}, TypeError, id="str-metric"),
        pytest.param(3, {"quant": "f32"}, TypeError, id="str-quant"),
        pytest.param(3, {"metric": Metric.HAMMING}, ValueError, id="hamming-f32"),
        pytest.param(3, {"quant": Quant.B1}, ValueError, id="cos-b1"),
    ],
)
def test_invalid_create_options_are_rejected_when_built(
    dim: int,
    options: dict[str, Any],
    error: type[Exception],
) -> None:
    # Given: Create options the server cannot accept.

    # When / Then: Building the options fails, before any exchange exists.
    with pytest.raises(error):
        VectorOptions(dim=dim, **options)


def test_create_options_require_dim() -> None:
    # Given / When / Then: Options without a dim cannot be built.
    with pytest.raises(TypeError):
        VectorOptions()  # ty: ignore[missing-argument] # missing on purpose


def test_create_rejects_options_of_another_type() -> None:
    # Given: The options as a dict instead of VectorOptions.
    options: Any = {"dim": 3}

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(TypeError):
        prepare_vop_create(b"idx", options)


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
    query: Sequence[float] | bytes | str,
    count: int = 5,
    filter: str | None = None,
    with_meta: bool = False,
) -> bytes:
    exchange = prepare_vop_search(b"idx", query, count, filter, with_meta)
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
    ("query", "count", "filter", "error"),
    [
        pytest.param("a b", 5, None, ValueError, id="space-vkey"),
        pytest.param([], 5, None, ValueError, id="empty-vector"),
        pytest.param(b"\x00\x00\x80", 5, None, ValueError, id="partial-f32-bytes"),
        pytest.param([math.nan], 5, None, ValueError, id="nan"),
        pytest.param("v", 0, None, ValueError, id="zero-count"),
        pytest.param("v", COUNT_RANGE.stop, None, ValueError, id="too-large-count"),
        pytest.param("v", True, None, TypeError, id="bool-count"),
        pytest.param("v", 5, "", ValueError, id="empty-filter"),
        pytest.param(
            "v", 5, "a" * (MAX_FILTER_LENGTH + 1), ValueError, id="long-filter"
        ),
        pytest.param("v", 5, b"n = 1", TypeError, id="bytes-filter"),
    ],
)
def test_invalid_search_is_rejected_before_iteration(
    query: Any,
    count: int,
    filter: Any,
    error: type[Exception],
) -> None:
    # Given: A search the server cannot accept.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(error):
        prepare_vop_search(b"idx", query, count, filter, False)
