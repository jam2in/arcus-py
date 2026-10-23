"""Public exceptions raised by the Arcus clients."""


class ArcusError(Exception):
    """Base class for every error raised by arcus."""


class ArcusConnectionError(ArcusError):
    """
    A ZooKeeper or cache node connection failed or was lost,
    or a cache node sent a response that violates the protocol.
    """


class ArcusTimeoutError(ArcusError, TimeoutError):
    """An Arcus operation exceeded its time limit."""


class ArcusDecodeError(ArcusError):
    """A cached value was received but could not be decoded."""


class ArcusClientError(ArcusError):
    """The cache server rejected the request as invalid (CLIENT_ERROR)."""


class ArcusServerError(ArcusError):
    """The cache server failed to process the request (SERVER_ERROR)."""
