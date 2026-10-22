"""Public exceptions raised by the Arcus clients."""


class ArcusError(Exception):
    """Base class for every error raised by arcus."""


class ArcusConnectionError(ArcusError):
    """A ZooKeeper or cache node connection failed or was lost."""


class ArcusTimeoutError(ArcusError, TimeoutError):
    """An Arcus operation exceeded its time limit."""


class ArcusDecodeError(ArcusError):
    """A cached value was received but could not be decoded."""
