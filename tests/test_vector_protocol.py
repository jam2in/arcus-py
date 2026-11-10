from typing import Any

import pytest
from support.drive import drive

from arcus._command.vector import (
    DIM_RANGE,
    EF_RANGE,
    M_RANGE,
    Metric,
    Quant,
    VectorOptions,
    prepare_vop_create,
)


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
