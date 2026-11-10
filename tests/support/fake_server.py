"""A local server that speaks just enough of the text protocol for the tests."""

import socketserver
import threading
from io import BufferedIOBase
from types import TracebackType
from typing import Self


class FakeServer:
    def __init__(self) -> None:
        self.items: dict[bytes, bytes] = {}
        # The vkeys of each vector index, with the metadata JSON each was upserted with.
        self.vectors: dict[bytes, dict[bytes, bytes]] = {}
        # Keys answered with this line, after which the server hangs up.
        self.errors: dict[bytes, bytes] = {}
        self.connections = 0
        self._lock = threading.Lock()

        fake = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                fake._serve(self.rfile, self.wfile)

        self._server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        port = self._server.server_address[1]
        self.address = f"127.0.0.1:{port}"

    def __enter__(self) -> Self:
        threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        ).start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._server.shutdown()
        self._server.server_close()

    def _serve(self, rfile: BufferedIOBase, wfile: BufferedIOBase) -> None:
        with self._lock:
            self.connections += 1

        while line := rfile.readline():
            command, key, *fields = line.rstrip(b"\r\n").split(b" ")
            if key in self.errors:
                wfile.write(self.errors[key] + b"\r\n")
                return

            if command == b"get":
                value = self.items.get(key)
                if value is not None:
                    wfile.write(b"VALUE %s 0 %d\r\n%s\r\n" % (key, len(value), value))
                wfile.write(b"END\r\n")
            elif command == b"set":
                size = int(fields[2])
                self.items[key] = rfile.read(size + 2)[:size]
                wfile.write(b"STORED\r\n")
            elif command == b"vop" and key == b"create":
                index = fields[0]
                if index in self.items:
                    wfile.write(b"EXISTS\r\n")
                else:
                    self.items[index] = b""
                    self.vectors[index] = {}
                    wfile.write(b"CREATED\r\n")
            elif command == b"vop" and key == b"upsert":
                index, vkey, size, *options = fields
                rfile.read(int(size) + 2)
                meta = b"{}"
                if options[:1] == [b"meta"]:
                    meta = rfile.read(int(options[1]) + 2)[:-2]
                if index in self.vectors:
                    self.vectors[index][vkey] = meta
                    wfile.write(b"STORED\r\n")
                else:
                    wfile.write(b"NOT_FOUND\r\n")
            elif command == b"vop" and key == b"search":
                # Every element scores 0; only the framing matters to the client.
                index, source, value, k, *options = fields
                if source == b"vector":
                    rfile.read(int(value) + 2)
                if b"filter" in options:
                    rfile.read(int(options[options.index(b"filter") + 1]) + 2)
                if index not in self.vectors:
                    wfile.write(b"NOT_FOUND\r\n")
                    continue
                found = list(self.vectors[index].items())[: int(k)]
                wfile.write(b"VECTORS %d\r\n" % len(found))
                for vkey, meta in found:
                    if b"withmeta" in options:
                        wfile.write(b"%s 0 %d\r\n%s\r\n" % (vkey, len(meta), meta))
                    else:
                        wfile.write(b"%s 0\r\n" % vkey)
                wfile.write(b"END\r\n")
