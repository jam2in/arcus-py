from collections.abc import Callable

from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NoNodeError, OperationTimeoutError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import WatchedEvent

from arcus import ArcusConnectionError
from arcus._logging import logger


def parse_cache_list(children: list[str]) -> list[str]:
    addresses: set[str] = set()

    for child in children:
        address = child.partition("-")[0]
        host, _, port = address.rpartition(":")

        if (
            not host
            or not port
            or not port.isascii()
            or not port.isdecimal()
            or not 1 <= int(port) <= 65535
        ):
            logger.warning(f"Ignoring invalid cache node {child}")
            continue

        addresses.add(address)

    return sorted(addresses)


class ArcusDiscovery:
    def __init__(
        self,
        zk_hosts: str,
        service_code: str,
        timeout: float = 10.0,
    ) -> None:
        self._client: KazooClient = KazooClient(hosts=zk_hosts)
        self._path: str = f"/arcus/cache_list/{service_code}"
        self._timeout: float = timeout
        self._closed: bool = False

        # Assigned in start() before registering the watch.
        self._on_change: Callable[[list[str]], None]

    def start(self, on_change: Callable[[list[str]], None]) -> list[str]:
        self._on_change = on_change

        try:
            self._client.start(timeout=self._timeout)
            children = self._client.get_children(
                path=self._path,
                watch=self._watch_children,
            )
            return parse_cache_list(children)
        except BaseException as error:
            self._closed = True
            self._cleanup()

            if isinstance(error, (KazooTimeoutError, OperationTimeoutError)):
                raise ArcusConnectionError(
                    "ZooKeeper discovery startup timed out"
                ) from error

            if isinstance(error, NoNodeError):
                raise ArcusConnectionError(
                    f"service path not found: {self._path}"
                ) from error

            if isinstance(error, (KazooException, OSError)):
                raise ArcusConnectionError(
                    "ZooKeeper discovery startup failed"
                ) from error

            raise

    def stop(self) -> None:
        self._closed = True
        self._cleanup()

    def _watch_children(self, _event: WatchedEvent) -> None:
        if self._closed:
            return

        try:
            children = self._client.get_children(
                path=self._path,
                watch=self._watch_children,
            )

            self._on_change(parse_cache_list(children))
        except Exception:
            logger.exception("Failed to update cache nodes")

    def _cleanup(self) -> None:
        try:
            self._client.stop()
        except Exception:
            logger.exception("Failed to stop ZooKeeper")

        try:
            self._client.close()
        except Exception:
            logger.exception("Failed to close ZooKeeper")
