"""Python Driver for Cache"""

from importlib.metadata import version

from .exceptions import (
    ArcusConnectionError,
    ArcusError,
    ArcusTimeoutError,
)

__version__ = version("arcus-py")

__all__ = [
    "ArcusConnectionError",
    "ArcusError",
    "ArcusTimeoutError",
    "__version__",
]
