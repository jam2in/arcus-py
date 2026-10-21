from bisect import bisect_left
from hashlib import md5
from struct import Struct
from threading import Lock

from arcus import ArcusConnectionError

_KETAMA_KEY_STRUCT = Struct("<I")
_KETAMA_POINTS_STRUCT = Struct("<4I")


def key_hash(key: bytes) -> int:
    digest = md5(key, usedforsecurity=False).digest()
    return _KETAMA_KEY_STRUCT.unpack_from(digest)[0]


def node_points(address: str) -> set[int]:
    points: set[int] = set()

    for index in range(40):
        data = f"{address}-{index}".encode()
        digest = md5(data, usedforsecurity=False).digest()
        points.update(_KETAMA_POINTS_STRUCT.unpack(digest))

    return points


class ArcusLocator:
    def __init__(self, addresses: list[str]) -> None:
        self._lock = Lock()
        self._ring: dict[int, set[str]] = {}
        self._points: list[int] = []
        self.update(set(addresses), set())

    def update(self, to_attach: set[str], to_delete: set[str]) -> None:
        with self._lock:
            for address in to_attach:
                self._insert_hash(address)
            for address in to_delete:
                self._remove_hash(address)

            self._points = sorted(self._ring)

    def get_node(self, key: bytes) -> str:
        point = key_hash(key)

        with self._lock:
            if not self._points:
                raise ArcusConnectionError("no cache nodes available")

            index = bisect_left(self._points, point)
            if index == len(self._points):
                index = 0

            return min(self._ring[self._points[index]])

    def _insert_hash(self, address: str) -> None:
        for point in node_points(address):
            self._ring.setdefault(point, set()).add(address)

    def _remove_hash(self, address: str) -> None:
        """Nodes to delete must be currently registered in the ring."""
        for point in set(node_points(address)):
            nodes = self._ring[point]
            nodes.remove(address)

            if not nodes:
                del self._ring[point]
