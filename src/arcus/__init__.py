"""Python Driver for Cache"""

from importlib.metadata import version

from .exceptions import (
    ArcusClientError,
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusError,
    ArcusServerError,
    ArcusTimeoutError,
)

__version__ = version("arcus-py")

__all__ = [
    "ArcusClientError",
    "ArcusConnectionError",
    "ArcusDecodeError",
    "ArcusError",
    "ArcusServerError",
    "ArcusTimeoutError",
    "__version__",
]
