"""Python Driver for Cache"""

from importlib.metadata import version

from ._command.vector import Metric, Quant, VectorOptions
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
    "Metric",
    "Quant",
    "VectorOptions",
    "__version__",
]
