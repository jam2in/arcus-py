import pytest

import arcus
from arcus.exceptions import (
    ArcusClientError,
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusError,
    ArcusOperationTimeoutError,
    ArcusPoolTimeoutError,
    ArcusServerError,
)

ALL_ERRORS = [
    ArcusClientError,
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusOperationTimeoutError,
    ArcusPoolTimeoutError,
    ArcusServerError,
]


@pytest.mark.parametrize("clazz", ALL_ERRORS)
def test_every_error_is_arcus_error(clazz: type[Exception]) -> None:
    assert issubclass(clazz, ArcusError)


def test_timeout_can_be_caught_as_builtin_timeout_error() -> None:
    with pytest.raises(TimeoutError):
        raise ArcusOperationTimeoutError("get timed out")


def test_timeout_message() -> None:
    assert str(ArcusOperationTimeoutError("get timed out")) == "get timed out"


@pytest.mark.parametrize("clazz", [ArcusError, *ALL_ERRORS])
def test_exported_from_package(clazz: type[Exception]) -> None:
    assert getattr(arcus, clazz.__name__) is clazz
    assert clazz.__name__ in arcus.__all__
