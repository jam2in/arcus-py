from bisect import bisect_left
from hashlib import md5
from struct import Struct

from arcus.exceptions import ArcusConnectionError

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
    """Ketama ring that never changes; build a new one for a new node list."""

    def __init__(self, addresses: list[str]) -> None:
        ring: dict[int, str] = {}
        # Sorted, so a point shared by several nodes keeps the smallest address.
        for address in sorted(set(addresses)):
            for point in node_points(address):
                ring.setdefault(point, address)

        self._ring = ring
        self._points = sorted(ring)

    def get_node(self, key: bytes) -> str:
        if not self._points:
            raise ArcusConnectionError("no cache nodes available")

        index = bisect_left(self._points, key_hash(key))
        if index == len(self._points):
            index = 0

        return self._ring[self._points[index]]
