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

    def __init__(self):
        self.closed = False
        self.finalised = None

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
        self.closed = True
        self.finalised = finalise_write


class FakeV3Gateway:
    """In-process fake of the proxy ``/secure/zarr`` (v3 / izarr) data plane.

    Serves the exact surface ``DatameshV3Store`` speaks:
    ``GET/HEAD/PUT/DELETE`` on ``/secure/zarr/{datasource}/{key}``, a listing
    endpoint (``GET /secure/zarr/{datasource}?op=list&prefix=``), and the two
    control POSTs ``/secure/zarr/_finalise/{ds}`` and ``/secure/zarr/_abort/{ds}``.

    Every request is appended to ``self.requests`` as ``(method, key)`` (key is
    the path relative to the datasource, or ``"_finalise"``/``"_abort"`` for the
    control POSTs) so tests can assert exact fetch patterns — e.g. that opening
    a consolidated group hits the root ``zarr.json`` once and never fetches a
    per-array ``zarr.json``. Set ``ignore_range=True`` to exercise the store's
    fetch-and-slice fallback (server returns the full object for a ranged GET).

    Set ``staged_writes=True`` to reproduce the proxy's real write-session
    read-visibility semantics (the default keeps the simple read-your-writes
    dict so the other tests are unaffected):

    * PUTs of keys ending in ``zarr.json`` apply IMMEDIATELY to the served
      state (eager metadata — matches the proxy committing ``zarr.json`` to the
      session branch);
    * all other PUTs (chunk data) go into a per-datasource ``pending`` staging
      dict that reads (GET/HEAD/list) do NOT see — matching per-worker write
      forks that are invisible to reads until finalise;
    * ``_finalise`` merges ``pending`` into the served state (last-write-wins
      per key); ``_abort`` drops ``pending``.

    Under these semantics a write algorithm that reads back a chunk it wrote
    earlier in the same session gets the stale branch copy — so the class of
    ordering bug the live proxy exposes becomes catchable offline.
    """

    def __init__(self, ignore_range=False, staged_writes=False):
        # datasource -> {key: bytes}  (served / committed branch state)
        self.stores = {}
        # datasource -> {key: bytes}  (uncommitted chunk PUTs, staged_writes only)
        self.pending = {}
        self.requests = []
        self.control = []  # (op, datasource)
        self.last_headers = {}
        self.ignore_range = ignore_range
        self.staged_writes = staged_writes

    def _store(self, ds):
        return self.stores.setdefault(ds, {})

    def _pending(self, ds):
        return self.pending.setdefault(ds, {})

    def seed_consolidated(self, ds_id, dataset):
        """Populate ``ds_id`` with a consolidated zarr-v3 store of ``dataset``.

        Writes the dataset to an in-memory zarr-v3 store with consolidated
        metadata (proxy behaviour: the root ``zarr.json`` embeds
        ``consolidated_metadata``) and copies every key into the fake.
        """
        import zarr
        from zarr.core.buffer import default_buffer_prototype

        mem = zarr.storage.MemoryStore()
        dataset.to_zarr(mem, mode="w", zarr_format=3, consolidated=True)
        proto = default_buffer_prototype()

        async def _dump():
            out = {}
            async for k in mem.list():
                buf = await mem.get(k, proto)
                out[k] = buf.to_bytes()
            return out

        self.stores[ds_id] = __import__("asyncio").run(_dump())

    def handler_factory(self):
        gw = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _route(self):
                parsed = urllib.parse.urlparse(self.path)
                query = urllib.parse.parse_qs(parsed.query)
                parts = parsed.path.split("/")
                # ['', 'secure', 'zarr', <seg0>, <seg1...>]
                seg0 = parts[3] if len(parts) > 3 else ""
                if seg0 in ("_finalise", "_abort"):
                    ds = parts[4] if len(parts) > 4 else ""
                    return "control", seg0, ds, "", query
                ds = seg0
                key = urllib.parse.unquote("/".join(parts[4:]))
                return "data", None, ds, key, query

            def do_GET(self):
                kind, _op, ds, key, query = self._route()
                store = gw._store(ds)
                if key == "" and "op" in query:
                    op = query["op"][0]
                    prefix = query.get("prefix", [""])[0]
                    gw.requests.append(("GET", f"?op={op}&prefix={prefix}"))
                    keys = _list_keys(store, op, prefix)
                    self._json({"keys": keys})
                    return
                gw.requests.append(("GET", key))
                gw.last_headers = dict(self.headers)
                if key not in store:
                    self.send_response(404)
                    self.end_headers()
                    return
                body = store[key]
                rng = self.headers.get("Range")
                if rng and not gw.ignore_range:
                    body, status = _apply_range(body, rng)
                else:
                    status = 200
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_HEAD(self):
                _kind, _op, ds, key, _q = self._route()
                store = gw._store(ds)
                gw.requests.append(("HEAD", key))
                self.send_response(200 if key in store else 404)
                self.end_headers()

            def do_PUT(self):
                _kind, _op, ds, key, _q = self._route()
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                if gw.staged_writes and not key.endswith("zarr.json"):
                    # Chunk data lands in the invisible write fork; only
                    # ``zarr.json`` metadata commits eagerly to the branch.
                    gw._pending(ds)[key] = body
                else:
                    gw._store(ds)[key] = body
                gw.requests.append(("PUT", key))
                gw.last_headers = dict(self.headers)
                self.send_response(201)
                self.end_headers()

            def do_DELETE(self):
                _kind, _op, ds, key, _q = self._route()
                gw.requests.append(("DELETE", key))
                targets = [gw._store(ds)]
                if gw.staged_writes:
                    targets.append(gw._pending(ds))
                for tgt in targets:
                    for k in list(tgt):
                        if k == key or k.startswith(key + "/") or key == "":
                            del tgt[k]
                self.send_response(204)
                self.end_headers()

            def do_POST(self):
                kind, op, ds, _key, _q = self._route()
                if kind == "control":
                    gw.control.append((op, ds))
                    gw.requests.append(("POST", op))
                    if gw.staged_writes:
                        pending = gw.pending.pop(ds, {})
                        if op == "_finalise":
                            # Commit staged chunk PUTs to the branch (LWW).
                            gw._store(ds).update(pending)
                        # ``_abort`` simply drops ``pending`` (popped above).
                    self._json({"status": "ok", "op": op})
                    return
                self.send_response(405)
                self.end_headers()

            def _json(self, obj):
                import json as _json_mod

                body = _json_mod.dumps(obj).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return H


def _list_keys(store, op, prefix):
    prefix = prefix.rstrip("/")
    base = f"{prefix}/" if prefix else ""
    if op == "list_dir":
        out = set()
        for k in store:
            if not k.startswith(base):
                continue
            rest = k[len(base):]
            out.add(rest.split("/", 1)[0] if "/" in rest else rest)
        return sorted(out)
    return sorted(k for k in store if k.startswith(base))


def _apply_range(body, rng):
    # rng like "bytes=start-end" | "bytes=start-" | "bytes=-suffix"
    spec = rng.split("=", 1)[1]
    start_s, _, end_s = spec.partition("-")
    if start_s == "":  # suffix
        sliced = body[-int(end_s):]
    elif end_s == "":  # offset
        sliced = body[int(start_s):]
    else:
        sliced = body[int(start_s):int(end_s) + 1]
    return sliced, 206


def start_v3_gateway(ignore_range=False, staged_writes=False):
    """Return (gateway, server, url); caller should ``server.shutdown()``."""
    gw = FakeV3Gateway(ignore_range=ignore_range, staged_writes=staged_writes)
    server = ThreadingHTTPServer(("127.0.0.1", 0), gw.handler_factory())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return gw, server, f"http://127.0.0.1:{server.server_address[1]}"


class FakeConnection:
    """Minimal stand-in for ``datamesh.connection.Connector``."""

    def __init__(self, url):
        self._gateway = url
        self._auth_headers = {"Authorization": "Token test"}
        self._verify = True
