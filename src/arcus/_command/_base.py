from abc import ABC, abstractmethod
from typing import TypeVar

from arcus._protocol.request import Exchange
from arcus.transcoder import ArcusTranscoder

T = TypeVar("T")


class SyncCommandExecutor(ABC):
    """Contract every sync command mixin relies on."""

    _transcoder: ArcusTranscoder

    @abstractmethod
    def _execute(self, key: bytes, exchange: Exchange[T]) -> T:
        raise NotImplementedError
