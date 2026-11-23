"""Public exceptions raised by the Arcus clients."""

from collections.abc import Sequence


class ArcusError(Exception):
    """Base class for every error raised by arcus."""


class ArcusConnectionError(ArcusError):
    """
    A ZooKeeper or cache node connection failed or was lost,
    or a cache node sent a response that violates the protocol.
    """


class ArcusOperationTimeoutError(ArcusError, TimeoutError):
    """A request was sent but did not complete in time; its outcome is unknown."""


class ArcusPoolTimeoutError(ArcusError, TimeoutError):
    """No cache connection became available within the pool timeout."""


class ArcusDecodeError(ArcusError):
    """A cached value was received but could not be decoded."""


class ArcusClientError(ArcusError):
    """The cache server rejected the request as invalid (CLIENT_ERROR)."""


class ArcusServerError(ArcusError):
    """The cache server failed to process the request (SERVER_ERROR)."""


class ArcusErrorGroup(ExceptionGroup[ArcusError], ArcusError):
    """A request to several cache nodes failed on some; exceptions hold each error."""

    def derive(self, excs: Sequence[ArcusError]) -> "ArcusErrorGroup":
        # except* splits the group with derive(); keep the split parts catchable
        # as ArcusError.
        return ArcusErrorGroup(self.message, excs)
