"""Python Driver for Cache"""

from importlib.metadata import version

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
    "__version__",
]
