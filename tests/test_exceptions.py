import pytest

import arcus
from arcus.exceptions import (
    ArcusClientError,
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusError,
    ArcusErrorGroup,
    ArcusOperationTimeoutError,
    ArcusPoolTimeoutError,
    ArcusServerError,
)

ALL_ERRORS = [
    ArcusClientError,
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusErrorGroup,
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


def test_error_group_is_caught_whole_or_by_kind() -> None:
    # Given: A group of two node errors of different kinds.
    connection_error = ArcusConnectionError("refused")
    timeout_error = ArcusOperationTimeoutError("timed out")
    group = ArcusErrorGroup("Failed", [connection_error, timeout_error])

    # When / Then: except ArcusError catches the whole group.
    with pytest.raises(ArcusError):
        raise group

    # When / Then: except* splits the group into ArcusErrorGroup parts by kind.
    caught: list[ArcusErrorGroup] = []
    try:
        raise group
    except* ArcusConnectionError as part:
        assert isinstance(part, ArcusErrorGroup)
        caught.append(part)
    except* ArcusOperationTimeoutError as part:
        assert isinstance(part, ArcusErrorGroup)
        caught.append(part)
    assert [part.exceptions for part in caught] == [
        (connection_error,),
        (timeout_error,),
    ]
