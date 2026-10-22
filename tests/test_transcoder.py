import datetime
import gzip
import json

import pytest
from pytest_mock import MockerFixture

from arcus.transcoder import ArcusTranscoder

T = ArcusTranscoder()


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("한글", id="string"),
        pytest.param("", id="empty-string"),
        pytest.param(True, id="bool"),
        pytest.param(-(2**63), id="int"),
        pytest.param(2**100, id="big-int"),
        pytest.param(1.5, id="float"),
        pytest.param({"name": "김철수", "tags": ["a", "b"]}, id="dict"),
        pytest.param([1, "a", None, False], id="list"),
    ],
)
def test_encode_as_utf8_json(value: object) -> None:
    # Given: A JSON-compatible value.

    # When: The value is encoded and decoded.
    data = T.encode(value)
    decoded = T.decode(data)

    # Then: It is stored as UTF-8 JSON and decodes to an equal value of the same type.
    assert data == json.dumps(value, ensure_ascii=False).encode()
    assert decoded == value
    assert type(decoded) is type(value)


def test_encode_compresses_large_value() -> None:
    # Given: A compressible value larger than the compression threshold.
    value = "a" * (16 * 1024)

    # When: The value is encoded.
    data = T.encode(value)

    # Then: It is stored gzip-compressed and decodes back to the original.
    assert gzip.decompress(data) == json.dumps(value).encode()
    assert T.decode(data) == value


def test_encode_keeps_value_at_threshold_uncompressed() -> None:
    # Given: A value whose JSON is exactly the compression threshold.
    value = "a" * (16 * 1024 - 2)

    # When: The value is encoded.
    data = T.encode(value)

    # Then: It is stored as plain JSON.
    assert data == json.dumps(value).encode()
    assert len(data) == 16 * 1024


@pytest.mark.parametrize(
    ("value", "error"),
    [
        pytest.param(None, TypeError, id="none"),
        pytest.param(b"x", TypeError, id="bytes"),
        pytest.param(datetime.datetime(2026, 10, 22), TypeError, id="datetime"),
        pytest.param({1, 2}, TypeError, id="set"),
        pytest.param(object(), TypeError, id="object"),
    ],
)
def test_encode_rejects_non_json_value(value: object, error: type[Exception]) -> None:
    # Given: A value that JSON cannot represent, or None.

    # When: The value is encoded.
    with pytest.raises(error):
        T.encode(value)

    # Then: The matching error is raised.


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"not json", id="invalid-json"),
        pytest.param(b"\xff", id="invalid-utf8"),
        pytest.param(b"\x1f\x8bnot gzip", id="corrupt-gzip"),
    ],
)
def test_decode_rejects_invalid_data(data: bytes) -> None:
    # Given: Bytes that are not JSON or valid gzip.

    # When: The value is decoded.
    with pytest.raises(ValueError):
        T.decode(data)

    # Then: A ValueError is raised for the protocol layer to wrap.


def test_encode_skips_compression_without_gain(mocker: MockerFixture) -> None:
    # Given: A large value whose compressed form would not be smaller.
    value = "a" * (16 * 1024)
    plain = json.dumps(value).encode()
    mocker.patch("arcus.transcoder.zlib.compress", return_value=b"x" * len(plain))

    # When: The value is encoded.
    data = T.encode(value)

    # Then: The plain JSON is stored.
    assert data == plain


@pytest.mark.parametrize(
    ("value", "decoded"),
    [
        pytest.param((1, 2), [1, 2], id="tuple-becomes-list"),
        pytest.param({1: "a"}, {"1": "a"}, id="int-key-becomes-str"),
    ],
)
def test_json_conversions_are_lossy(value: object, decoded: object) -> None:
    # Given: A value that JSON represents with a different Python type.

    # When: The value is encoded and decoded.
    result = T.decode(T.encode(value))

    # Then: The JSON form comes back, documenting the conversion.
    assert result == decoded
