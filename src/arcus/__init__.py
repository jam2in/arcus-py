"""Python Driver for Arcus cache"""

from importlib.metadata import version

from ._command.vector import Metric, Quant, VectorOptions
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
    "Metric",
    "Quant",
    "VectorOptions",
    "__version__",
]
