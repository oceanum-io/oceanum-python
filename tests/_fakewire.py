"""In-process threaded fake of the Datamesh v2 gateway zarr wire.

Serves the exact protocol ``DatameshV2WireStore`` speaks:
``GET/HEAD/POST/DELETE`` on ``/zarr[/query]/{datasource}/{key}`` plus an HTML
autoindex for directory listings. Used by the unit tests so nothing hits a
live service.
"""
import html
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeGateway:
    """Keys are stored in a dict, keyed relative to ``{api}/{datasource}``."""

    def __init__(self):
        # (api, datasource) -> {key: bytes}
        self.stores = {}

    def _store(self, api, ds):
        return self.stores.setdefault((api, ds), {})

    def handler_factory(self):
        gw = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _route(self):
                path = urllib.parse.urlparse(self.path).path
                # /zarr/{ds}/{key}  or  /zarr/query/{ds}/{key}
                parts = path.split("/")
                # ['', 'zarr', ...]
                if len(parts) >= 3 and parts[1] == "zarr" and parts[2] == "query":
                    api = "query"
                    ds = parts[3] if len(parts) > 3 else ""
                    key = "/".join(parts[4:])
                else:
                    api = "zarr"
                    ds = parts[2] if len(parts) > 2 else ""
                    key = "/".join(parts[3:])
                return api, ds, urllib.parse.unquote(key)

            def do_GET(self):
                api, ds, key = self._route()
                store = gw._store(api, ds)
                if key == "" or key.endswith("/"):
                    self._autoindex(store, key)
                    return
                if key in store:
                    body = store[key]
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self.send_response(404)
                    self.end_headers()

            def _autoindex(self, store, prefix):
                prefix = prefix.rstrip("/")
                base = f"{prefix}/" if prefix else ""
                children, subdirs = set(), set()
                for k in store:
                    if not k.startswith(base):
                        continue
                    rest = k[len(base):]
                    if "/" in rest:
                        subdirs.add(rest.split("/", 1)[0])
                    else:
                        children.add(rest)
                links = "".join(
                    f'<a href="{html.escape(c)}">{c}</a>\n' for c in sorted(children)
                )
                links += "".join(
                    f'<a href="{html.escape(d)}/">{d}/</a>\n' for d in sorted(subdirs)
                )
                body = (
                    f'<html><body>\n<a href="../">../</a>\n{links}</body></html>'
                ).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_HEAD(self):
                api, ds, key = self._route()
                store = gw._store(api, ds)
                self.send_response(200 if key in store else 404)
                self.end_headers()

            def do_POST(self):
                api, ds, key = self._route()
                store = gw._store(api, ds)
                length = int(self.headers.get("Content-Length", 0))
                store[key] = self.rfile.read(length)
                self.send_response(200)
                self.end_headers()

            def do_DELETE(self):
                api, ds, key = self._route()
                store = gw._store(api, ds)
                if key == "":
                    store.clear()
                else:
                    for k in list(store):
                        if k == key or k.startswith(key + "/"):
                            del store[k]
                self.send_response(204)
                self.end_headers()

        return H


def start_gateway():
    """Return (gateway, server, url); caller should ``server.shutdown()``."""
    gw = FakeGateway()
    server = ThreadingHTTPServer(("127.0.0.1", 0), gw.handler_factory())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return gw, server, f"http://127.0.0.1:{server.server_address[1]}"


class FakeSession:
    """Minimal stand-in for ``datamesh.session.Session``."""

    id = "test-session-id"

    def add_header(self, headers):
        return {**headers, "X-DATAMESH-SESSIONID": self.id}

    @property
    def header(self):
        return {"X-DATAMESH-SESSIONID": self.id}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self, finalise_write=False):
        pass


class FakeConnection:
    """Minimal stand-in for ``datamesh.connection.Connector``."""

    def __init__(self, url):
        self._gateway = url
        self._auth_headers = {"Authorization": "Token test"}
        self._verify = True
