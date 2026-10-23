"""Drive an exchange against an in-memory response, without sockets."""

from typing import TypeVar

from arcus import ArcusConnectionError
from arcus._protocol.request import Exchange, ReadExactly, ReadLine, Write

T = TypeVar("T")


def drive(exchange: Exchange[T], response: bytes) -> tuple[T, bytes]:
    """Run the exchange and return its result and every byte it sent."""
    buffer = bytearray(response)
    sent = bytearray()

    try:
        op = next(exchange)
        while True:
            match op:
                case Write(data):
                    sent += data
                    op = exchange.send(b"")
                case ReadLine():
                    end = buffer.find(b"\r\n")
                    if end < 0:
                        raise ArcusConnectionError("eof")
                    line = bytes(buffer[:end])
                    del buffer[: end + 2]
                    op = exchange.send(line)
                case ReadExactly(size):
                    if len(buffer) < size:
                        raise ArcusConnectionError("eof")
                    data = bytes(buffer[:size])
                    del buffer[:size]
                    op = exchange.send(data)
    except StopIteration as stop:
        return stop.value, bytes(sent)
