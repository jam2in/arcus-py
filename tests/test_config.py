import dataclasses

import pytest

from arcus._config import ClientConfig

ZK_HOSTS = "127.0.0.1:2181"
SERVICE_CODE = "test"

TIMEOUT_FIELDS = ["connect_timeout", "socket_timeout", "pool_timeout", "zk_timeout"]


def make_config(**overrides: object) -> ClientConfig:
    arguments: dict[str, object] = {
        "zk_hosts": ZK_HOSTS,
        "service_code": SERVICE_CODE,
        **overrides,
    }
    return ClientConfig(**arguments)  # ty: ignore[invalid-argument-type]


def test_defaults() -> None:
    # Given: Only the required settings.

    # When: The config is created.
    config = ClientConfig(ZK_HOSTS, SERVICE_CODE)

    # Then: The documented defaults are used.
    assert config.max_connections_per_node == 8
    assert config.connect_timeout == 5.0
    assert config.socket_timeout == 5.0
    assert config.pool_timeout == 5.0
    assert config.zk_timeout == 10.0


@pytest.mark.parametrize("timeout", [1, 0.001], ids=["int", "float"])
@pytest.mark.parametrize("field", TIMEOUT_FIELDS)
def test_positive_timeout_is_accepted(field: str, timeout: float) -> None:
    # Given: A positive int or float timeout.

    # When: The config is created.
    config = make_config(**{field: timeout})

    # Then: The value is kept as given.
    assert getattr(config, field) == timeout


def test_config_is_immutable() -> None:
    # Given: A valid config.
    config = make_config()

    # When: A setting is reassigned.
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.zk_timeout = 1.0  # ty: ignore[invalid-assignment]

    # Then: The original value is kept.
    assert config.zk_timeout == 10.0


@pytest.mark.parametrize(
    ("value", "error"),
    [
        pytest.param(None, TypeError, id="none"),
        pytest.param(b"test", TypeError, id="bytes"),
        pytest.param("", ValueError, id="empty"),
        pytest.param("   ", ValueError, id="blank"),
    ],
)
@pytest.mark.parametrize("field", ["zk_hosts", "service_code"])
def test_invalid_string_is_rejected(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    # Given: A required string setting with an invalid value.

    # When: The config is created.
    with pytest.raises(error, match=field):
        make_config(**{field: value})

    # Then: The error names the setting.


@pytest.mark.parametrize(
    ("value", "error"),
    [
        pytest.param(True, TypeError, id="bool"),
        pytest.param(1.5, TypeError, id="float"),
        pytest.param("8", TypeError, id="str"),
        pytest.param(0, ValueError, id="zero"),
        pytest.param(-1, ValueError, id="negative"),
    ],
)
def test_invalid_max_connections_is_rejected(
    value: object,
    error: type[Exception],
) -> None:
    # Given: An invalid connection limit.

    # When: The config is created.
    with pytest.raises(error, match="max_connections_per_node"):
        make_config(max_connections_per_node=value)

    # Then: The error names the setting.


@pytest.mark.parametrize(
    ("value", "error"),
    [
        pytest.param(None, TypeError, id="none"),
        pytest.param(True, TypeError, id="bool"),
        pytest.param("5", TypeError, id="str"),
        pytest.param(0, ValueError, id="zero"),
        pytest.param(-1.0, ValueError, id="negative"),
        pytest.param(float("inf"), ValueError, id="inf"),
        pytest.param(float("nan"), ValueError, id="nan"),
        pytest.param(-(10**400), ValueError, id="huge-negative-int"),
    ],
)
@pytest.mark.parametrize("field", TIMEOUT_FIELDS)
def test_invalid_timeout_is_rejected(
    field: str,
    value: object,
    error: type[Exception],
) -> None:
    # Given: A timeout setting with an invalid value.

    # When: The config is created.
    with pytest.raises(error, match=field):
        make_config(**{field: value})

    # Then: The error names the setting.
