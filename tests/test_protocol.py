import pytest
from support.drive import drive
from support.response_cases import CASES, Case

from arcus import ArcusConnectionError
from arcus._command.kv import MAX_KEY_LENGTH, prepare_get, prepare_set
from arcus._protocol.framing import read_data_block
from arcus.transcoder import ArcusTranscoder

T = ArcusTranscoder()


def test_get_request_bytes() -> None:
    # Given: A get exchange for a key.
    exchange = prepare_get("k", T)

    # When: The exchange is driven.
    _, sent = drive(exchange, b"END\r\n")

    # Then: The get command line is sent.
    assert sent == b"get k\r\n"


def test_set_request_bytes() -> None:
    # Given: A set exchange with an expiration time.
    exchange = prepare_set("k", "v", 10, T)

    # When: The exchange is driven.
    _, sent = drive(exchange, b"STORED\r\n")

    # Then: The JSON value is sent with flags 0, the exptime and its length.
    assert sent == b'set k 0 10 3\r\n"v"\r\n'


def test_max_length_key_is_accepted() -> None:
    # Given: A key of exactly the maximum length.
    key = "k" * MAX_KEY_LENGTH

    # When: A get exchange is driven.
    _, sent = drive(prepare_get(key, T), b"END\r\n")

    # Then: The full key is sent.
    assert sent == b"get " + key.encode() + b"\r\n"


@pytest.mark.parametrize(
    ("key", "error"),
    [
        pytest.param(b"k", TypeError, id="bytes"),
        pytest.param("", ValueError, id="empty"),
        pytest.param("a b", ValueError, id="space"),
        pytest.param("a\r\nb", ValueError, id="crlf"),
        pytest.param("a\x00", ValueError, id="nul"),
        pytest.param("a\x7f", ValueError, id="del"),
        pytest.param("가" * 1334, ValueError, id="too-long"),
    ],
)
def test_invalid_key_is_rejected_before_iteration(
    key: object,
    error: type[Exception],
) -> None:
    # Given: A key that cannot be sent on the protocol line.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(error):
        prepare_get(key, T)  # ty: ignore[invalid-argument-type] # invalid on purpose


@pytest.mark.parametrize(
    "exptime",
    [
        pytest.param(True, id="bool"),
        pytest.param(1.5, id="float"),
    ],
)
def test_non_int_exptime_is_rejected(exptime: object) -> None:
    # Given: An expiration time that is not an int.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(TypeError):
        prepare_set("k", "v", exptime, T)  # ty: ignore[invalid-argument-type] # invalid on purpose


def test_none_value_is_rejected_before_iteration() -> None:
    # Given: None, which would make a None from get() ambiguous.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(TypeError):
        prepare_set("k", None, 0, T)


def test_data_block_may_contain_crlf() -> None:
    # Given: A data block written by another client that contains CRLF and END.
    response = b"\r\nEND\r\n" + b"\r\n"

    # When: The block is read by its length.
    block, _ = drive(read_data_block(7), response)

    # Then: The embedded CRLF is part of the data, not a line boundary.
    assert block == b"\r\nEND\r\n"


def test_data_block_without_crlf_is_rejected() -> None:
    # Given: A data block that is not followed by CRLF.
    response = b"abcXX"

    # When / Then: Reading the block fails as a protocol violation.
    with pytest.raises(ArcusConnectionError):
        drive(read_data_block(3), response)


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_response_case(case: Case) -> None:
    # Given: A shared response case.

    # When: The exchange is driven against the response.
    if case.error is None:
        result, _ = drive(case.make(), case.response)

        # Then: The expected result is returned.
        assert result == case.result
        assert type(result) is type(case.result)
        return

    with pytest.raises(case.error) as exc_info:
        drive(case.make(), case.response)

    # Then: The exact error is raised.
    assert type(exc_info.value) is case.error
