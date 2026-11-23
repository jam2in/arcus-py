import pytest
from support.drive import drive
from support.response_cases import CASES, Case

from arcus import ArcusConnectionError
from arcus._command.flush import prepare_flush
from arcus._command.kv import prepare_get, prepare_set
from arcus._protocol.framing import read_data_block
from arcus._validation import (
    MAX_EXPTIME,
    MAX_KEY_LENGTH,
    MAX_PREFIX_LENGTH,
    STICKY_EXPTIME,
    validate_key,
    validate_prefix,
)
from arcus.transcoder import ArcusTranscoder

T = ArcusTranscoder()


def test_get_request_bytes() -> None:
    # Given: A get exchange for a key.
    exchange = prepare_get(b"k", T)

    # When: The exchange is driven.
    _, sent = drive(exchange, b"END\r\n")

    # Then: The get command line is sent.
    assert sent == b"get k\r\n"


def test_set_request_bytes() -> None:
    # Given: A set exchange with an expiration time.
    exchange = prepare_set(b"k", "v", 10, T)

    # When: The exchange is driven.
    _, sent = drive(exchange, b"STORED\r\n")

    # Then: The JSON value is sent with flags 0, the exptime and its length.
    assert sent == b'set k 0 10 3\r\n"v"\r\n'


def test_max_length_key_is_accepted() -> None:
    # Given: A key of exactly the maximum length.
    key = "k" * MAX_KEY_LENGTH

    # When: The key is validated and a get exchange is driven.
    validate_key(key)
    _, sent = drive(prepare_get(key.encode(), T), b"END\r\n")

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
def test_invalid_key_is_rejected(key: object, error: type[Exception]) -> None:
    # Given: A key that cannot be sent on the protocol line.

    # When / Then: Validating the key fails before any exchange is prepared.
    with pytest.raises(error):
        validate_key(key)  # ty: ignore[invalid-argument-type] # invalid on purpose


def test_none_value_is_rejected_before_iteration() -> None:
    # Given: None, which would make a None from get() ambiguous.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(TypeError):
        prepare_set(b"k", None, 0, T)


@pytest.mark.parametrize(
    ("exptime", "sent_exptime"),
    [
        pytest.param(STICKY_EXPTIME, b"-1", id="sticky"),
        pytest.param(MAX_EXPTIME, b"9223372036854775807", id="max"),
    ],
)
def test_exptime_bounds_are_accepted(exptime: int, sent_exptime: bytes) -> None:
    # Given: An exptime at either end of the range the server accepts.
    exchange = prepare_set(b"k", "v", exptime, T)

    # When: The exchange is driven.
    _, sent = drive(exchange, b"STORED\r\n")

    # Then: The exptime is sent unchanged.
    assert sent == b"set k 0 " + sent_exptime + b' 3\r\n"v"\r\n'


@pytest.mark.parametrize(
    ("exptime", "error"),
    [
        pytest.param(True, TypeError, id="bool"),
        pytest.param(1.5, TypeError, id="float"),
        pytest.param("10", TypeError, id="str"),
        pytest.param(STICKY_EXPTIME - 1, ValueError, id="below-sticky"),
        pytest.param(MAX_EXPTIME + 1, ValueError, id="too-large"),
    ],
)
def test_invalid_exptime_is_rejected_before_iteration(
    exptime: object,
    error: type[Exception],
) -> None:
    # Given: An exptime the server cannot accept.

    # When / Then: Preparing the exchange fails before any byte is sent.
    with pytest.raises(error, match="Exptime"):
        prepare_set(b"k", "v", exptime, T)  # ty: ignore[invalid-argument-type] # invalid on purpose


@pytest.mark.parametrize(
    ("prefix", "delay", "request_bytes"),
    [
        pytest.param(None, 0, b"flush_all 0\r\n", id="all"),
        pytest.param(b"p", 10, b"flush_prefix p 10\r\n", id="prefix"),
        pytest.param(b"", 0, b"flush_prefix <null> 0\r\n", id="null-prefix"),
        pytest.param(
            None,
            MAX_EXPTIME,
            b"flush_all 9223372036854775807\r\n",
            id="max-delay",
        ),
    ],
)
def test_flush_request_bytes(
    prefix: bytes | None,
    delay: int,
    request_bytes: bytes,
) -> None:
    # Given: A factory of flush exchanges.
    create_exchange = prepare_flush(prefix, delay)

    # When: Two exchanges from it are driven.
    _, first = drive(create_exchange(), b"OK\r\n")
    _, second = drive(create_exchange(), b"OK\r\n")

    # Then: Each sends the same flush command line.
    assert first == second == request_bytes


@pytest.mark.parametrize(
    ("delay", "error"),
    [
        pytest.param(True, TypeError, id="bool"),
        pytest.param(1.5, TypeError, id="float"),
        pytest.param(-1, ValueError, id="negative"),
        pytest.param(MAX_EXPTIME + 1, ValueError, id="too-large"),
    ],
)
def test_invalid_flush_delay_is_rejected(
    delay: object,
    error: type[Exception],
) -> None:
    # Given: A delay the server cannot accept.

    # When / Then: Preparing the flush fails before any exchange is made.
    with pytest.raises(error, match="Delay"):
        prepare_flush(None, delay)  # ty: ignore[invalid-argument-type] # invalid on purpose


def test_prefix_bounds_are_accepted() -> None:
    # Given / When / Then: The empty prefix and the longest prefix are valid.
    validate_prefix("")
    validate_prefix("p" * MAX_PREFIX_LENGTH)


@pytest.mark.parametrize(
    ("prefix", "error"),
    [
        pytest.param(b"p", TypeError, id="bytes"),
        pytest.param("a b", ValueError, id="space"),
        pytest.param("a\r\nb", ValueError, id="crlf"),
        pytest.param("p" * (MAX_PREFIX_LENGTH + 1), ValueError, id="too-long"),
    ],
)
def test_invalid_prefix_is_rejected(prefix: object, error: type[Exception]) -> None:
    # Given: A prefix that cannot be sent on the protocol line.

    # When / Then: Validating the prefix fails.
    with pytest.raises(error, match="Prefix"):
        validate_prefix(prefix)  # ty: ignore[invalid-argument-type] # invalid on purpose


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
