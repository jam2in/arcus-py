"""Python Driver for Arcus cache"""

from importlib.metadata import version

from .aio import AsyncArcus
from .client import Arcus
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
    "Arcus",
    "ArcusClientError",
    "ArcusConnectionError",
    "ArcusDecodeError",
    "ArcusError",
    "ArcusOperationTimeoutError",
    "ArcusPoolTimeoutError",
    "ArcusServerError",
    "AsyncArcus",
    "__version__",
]
