"""
replica.py - one inventory replica (primary or secondary).

Run it like:
    python3 replica.py --role secondary --port 5002
    python3 replica.py --role primary   --port 5001 --secondary http://localhost:5002

Primary   : take the writes, then send them to secondary in background (async).
Secondary : only take the replicated writes from primary, and serve reads.
"""

import argparse
import json
import queue
import threading
import time
import urllib.request
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def log(role, msg):
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] [{role.upper():9}] {msg}", flush=True)


class Replica:
    def __init__(self, role, secondary_url=None):
        self.role = role
        self.secondary_url = secondary_url
        self.data = {"stock": 10}     # starting inventory
        self.version = 0              # every write increase this
        self.lock = threading.Lock()

        # only used by primary
        self.repl_queue = queue.Queue()
        self.repl_paused = threading.Event()   # when set, replication is hold
        self.repl_delay = 0.0                  # extra delay before send each update

        if role == "primary":
            t = threading.Thread(target=self._replication_worker, daemon=True)
            t.start()

    # ---------- reads / writes ----------
    def read(self, key):
        with self.lock:
            return self.data.get(key), self.version

    def write(self, key, value):
        """Only primary accept client writes."""
        with self.lock:
            self.version += 1
            self.data[key] = value
            ver = self.version
        log(self.role, f"WRITE {key}={value} (version {ver})")
        # we don't wait for secondary here -> asynchronous replication
        self.repl_queue.put({"key": key, "value": value, "version": ver})
        log(self.role, f"queued update v{ver} for secondary "
                       f"(queue size={self.repl_queue.qsize()})")
        return ver

    def apply_replicated(self, key, value, version):
        """Secondary apply the update which come from primary."""
        with self.lock:
            if version <= self.version:
                log(self.role, f"ignore old update v{version}")
                return self.version
            self.data[key] = value
            self.version = version
        log(self.role, f"REPLICATED {key}={value} (version {version})")
        return version

    # ---------- background replication (primary only) ----------
    def _replication_worker(self):
        while True:
            update = self.repl_queue.get()
            # if paused, just wait here until somebody resume
            if self.repl_paused.is_set():
                log(self.role, f"replication is PAUSED, holding v{update['version']}")
            while self.repl_paused.is_set():
                time.sleep(0.2)
            if self.repl_delay > 0:
                time.sleep(self.repl_delay)
            self._send_to_secondary(update)

    def _send_to_secondary(self, update):
        body = json.dumps(update).encode()
        for attempt in range(1, 6):
            try:
                req = urllib.request.Request(
                    self.secondary_url + "/replicate", data=body,
                    headers={"Content-Type": "application/json"}, method="POST")
                urllib.request.urlopen(req, timeout=3).read()
                log(self.role, f"sent v{update['version']} "
                               f"({update['key']}={update['value']}) to secondary")
                return
            except Exception as e:
                log(self.role, f"send failed (try {attempt}): {e}")
                time.sleep(1)
        log(self.role, f"gave up on v{update['version']}")


def make_handler(replica):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):   # turn off default noisy http log
            pass

        def _reply(self, code, obj):
            out = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)

        def _body(self):
            n = int(self.headers.get("Content-Length", 0))
            return json.loads(self.rfile.read(n) or b"{}")

        def do_GET(self):
            if self.path.startswith("/stock"):
                value, ver = replica.read("stock")
                log(replica.role, f"READ stock -> {value} (version {ver})")
                self._reply(200, {"replica": replica.role, "stock": value,
                                  "version": ver})
            elif self.path == "/health":
                self._reply(200, {"ok": True, "role": replica.role})
            else:
                self._reply(404, {"error": "not found"})

        def do_POST(self):
            body = self._body()
            if self.path == "/stock":
                if replica.role != "primary":
                    self._reply(403, {"error": "writes only go to primary"})
                    return
                ver = replica.write("stock", int(body["value"]))
                self._reply(200, {"replica": "primary", "stock": body["value"],
                                  "version": ver})
            elif self.path == "/replicate" and replica.role == "secondary":
                ver = replica.apply_replicated(body["key"], body["value"],
                                               body["version"])
                self._reply(200, {"ok": True, "version": ver})
            elif self.path == "/replication/pause" and replica.role == "primary":
                replica.repl_paused.set()
                log(replica.role, ">>> replication to secondary PAUSED")
                self._reply(200, {"paused": True})
            elif self.path == "/replication/resume" and replica.role == "primary":
                replica.repl_paused.clear()
                log(replica.role, ">>> replication to secondary RESUMED")
                self._reply(200, {"paused": False})
            elif self.path == "/replication/delay" and replica.role == "primary":
                replica.repl_delay = float(body.get("seconds", 0))
                log(replica.role, f">>> replication delay set to "
                                  f"{replica.repl_delay}s")
                self._reply(200, {"delay": replica.repl_delay})
            else:
                self._reply(404, {"error": "not found"})

    return Handler


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--role", choices=["primary", "secondary"], required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--secondary", help="secondary url (primary only)")
    args = p.parse_args()

    if args.role == "primary" and not args.secondary:
        p.error("primary need --secondary url")

    replica = Replica(args.role, args.secondary)
    server = ThreadingHTTPServer(("localhost", args.port), make_handler(replica))
    log(args.role, f"started on port {args.port}, stock={replica.data['stock']}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log(args.role, "shutting down")


if __name__ == "__main__":
    main()
