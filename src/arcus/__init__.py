"""Python Driver for Cache"""

from importlib.metadata import version

from .exceptions import (
    ArcusConnectionError,
    ArcusDecodeError,
    ArcusError,
    ArcusTimeoutError,
)

__version__ = version("arcus-py")

__all__ = [
    "ArcusConnectionError",
    "ArcusDecodeError",
    "ArcusError",
    "ArcusTimeoutError",
    "__version__",
]
