#!/usr/bin/env python3
"""Throwaway stub daemon for pg_relay development.

Pretends to be the real (remote) daemon. ALL table functions and their
logic live here, never in the extension. Wire protocol:

    Read  :  GET  /<name>?args=<url-encoded-json>   (never cached)
    Write :  POST /<name>   body = <args json>
    Update:  PUT  /<name>   body = <args json>

Run it:   python3 stub/daemon.py
Test it:  curl 'localhost:8080/kv_get?args=%7B%22key%22%3A%22foo%22%7D'
          curl -X POST localhost:8080/kv_put -d '{"key":"foo","value":"bar"}'
          curl -X PUT localhost:8080/kv_put -d '{"key":"foo","value":"bar"}'
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

HOST, PORT = "127.0.0.1", 8080

# The daemon's "data source" — fake, in-memory, lost on restart.
STORE = {}


# ─── table function definitions live here, keyed by name ──────────────

def handle_read(name, args):
    if name == "kv_get":
        key = args.get("key", "")
        return [{"key": key, "value": STORE.get(key)}]
    return []


def handle_write(name, args):
    if name == "kv_put":
        key = args.get("key", "")
        value = args.get("value")
        STORE[key] = value              # the "bunch of updates" to the data source
        return [{"key": key, "value": value, "status": "ok"}]
    return []


# ─── HTTP plumbing (generic; doesn't know any function names) ─────────

class Handler(BaseHTTPRequestHandler):
    def _send(self, rows, extra_headers=None):
        payload = json.dumps({"rows": rows}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def _caller(self):
        return (
            f"role={self.headers.get('X-Pg-Role')!r} "
            f"app={self.headers.get('X-Pg-Application')!r} "
            f"pid={self.headers.get('X-Pg-Backend-Pid')!r}"
        )

    def do_GET(self):
        parsed = urlparse(self.path)
        name = parsed.path.lstrip("/")
        # args come in the JSON body (pg_relay_read_json) when one is sent,
        # otherwise from the ?args= query param (pg_relay_read).
        if int(self.headers.get("Content-Length", 0)) > 0:
            args = self._body_args()
        else:
            args = json.loads(parse_qs(parsed.query).get("args", ["{}"])[0])
        rows = handle_read(name, args)
        self._send(rows, {"Cache-Control": "no-store"})  # reads must not be cached
        print(f"GET  {name}({args}) => {len(rows)} row(s)  [{self._caller()}]")

    def _body_args(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_POST(self):
        name = urlparse(self.path).path.lstrip("/")
        args = self._body_args()
        rows = handle_write(name, args)
        self._send(rows)
        print(f"POST {name}({args}) => {len(rows)} row(s)  [{self._caller()}]  STORE={STORE}")

    def do_PUT(self):
        name = urlparse(self.path).path.lstrip("/")
        args = self._body_args()
        rows = handle_write(name, args)
        self._send(rows)
        print(f"PUT  {name}({args}) => {len(rows)} row(s)  [{self._caller()}]  STORE={STORE}")

    def log_message(self, *args):
        pass  # silence default per-request logging


if __name__ == "__main__":
    print(f"stub daemon on http://{HOST}:{PORT}")
    HTTPServer((HOST, PORT), Handler).serve_forever()
