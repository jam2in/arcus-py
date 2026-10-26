"""Python Driver for Cache"""

from importlib.metadata import version

from .exceptions import (
    ArcusClientError,
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusError,
    ArcusOperationTimeoutError,
    ArcusPoolTimeoutError,
    ArcusServerError,
)

__version__ = version("arcus-py")

__all__ = [
    "ArcusClientError",
    "ArcusConnectionError",
    "ArcusDecodeError",
    "ArcusError",
    "ArcusOperationTimeoutError",
    "ArcusPoolTimeoutError",
    "ArcusServerError",
    "__version__",
]
