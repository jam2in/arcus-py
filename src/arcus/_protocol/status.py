"""Error lines shared by every command."""

from typing import NoReturn

from arcus.exceptions import (
    ArcusClientError,
    ArcusConnectionError,
    ArcusError,
    ArcusServerError,
)


def raise_unexpected(line: bytes) -> NoReturn:
    """Raise the error for a line the command did not expect."""
    message = line.decode("utf-8", "replace")
    if line.startswith(b"CLIENT_ERROR"):
        raise ArcusClientError(message)
    if line.startswith(b"SERVER_ERROR"):
        raise ArcusServerError(message)
    if line.startswith(b"ERROR"):
        raise ArcusError(message)
    raise ArcusConnectionError(f"Unexpected response: {message}")
