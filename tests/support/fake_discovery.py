"""A discovery the tests control instead of ZooKeeper."""

import threading
from collections.abc import Callable


class FakeDiscovery:
    def __init__(
        self,
        nodes: list[str],
        *,
        error: BaseException | None = None,
        nodes_during_start: list[str] | None = None,
        block: bool = False,
    ) -> None:
        self.nodes = nodes
        self.error = error
        self.nodes_during_start = nodes_during_start
        self.entered = threading.Event()
        self.proceed = threading.Event()
        if not block:
            self.proceed.set()
        self.stopped = False
        self._on_change: Callable[[list[str]], None] | None = None

    def start(self, on_change: Callable[[list[str]], None]) -> list[str]:
        self._on_change = on_change
        self.entered.set()
        self.proceed.wait()

        if self.nodes_during_start is not None:
            on_change(self.nodes_during_start)
        if self.error is not None:
            raise self.error
        return list(self.nodes)

    def stop(self) -> None:
        self.stopped = True

    def change(self, nodes: list[str]) -> None:
        """Deliver a node list change the way the ZooKeeper watch would."""
        assert self._on_change is not None
        self._on_change(nodes)
