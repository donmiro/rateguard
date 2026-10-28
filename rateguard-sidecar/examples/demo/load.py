"""An unfair load balancer in front of a few sidecars, for compose.yaml.

TARGETS is host:port=rate, comma-separated: requests a second offered to
each sidecar, all on one key. Every second, prints what each sidecar
admitted, what the fleet did, and how many members the sidecars see.

Plain http.client with one kept-alive connection per sidecar, to keep up
with the rate; a service would use a client from sdk/ instead.
"""

import http.client
import json
import os
import threading
import time

KEY = os.environ.get("KEY", "api:tenant-42")
LIMIT = os.environ.get("LIMIT", "?")
TICK = 0.01


class Target:
    def __init__(self, spec):
        address, rate = spec.split("=")
        self.host, port = address.split(":")
        self.port = int(port)
        self.rate = int(rate)
        self.allowed = 0
        self.up = False
        self.lock = threading.Lock()

    def connect(self):
        return http.client.HTTPConnection(self.host, self.port, timeout=1)

    def run(self):
        body = json.dumps({"key": KEY})
        headers = {"Content-Type": "application/json"}
        conn = self.connect()
        start, sent = time.monotonic(), 0
        while True:
            # Requests due by now; a pause, such as a sidecar gone, is not
            # made up for afterwards.
            due = int((time.monotonic() - start) * self.rate)
            if due - sent > self.rate:
                sent = due - self.rate // 10
            try:
                while sent < due:
                    conn.request("POST", "/v1/check", body, headers)
                    answer = json.loads(conn.getresponse().read())
                    sent += 1
                    with self.lock:
                        self.allowed += answer["allowed"]
                self.up = True
            except (OSError, http.client.HTTPException, ValueError):
                self.up = False
                conn.close()
                time.sleep(0.5)
                conn = self.connect()
                continue
            time.sleep(TICK)

    def take(self):
        with self.lock:
            allowed, self.allowed = self.allowed, 0
        return allowed

    def cluster_size(self):
        try:
            conn = self.connect()
            conn.request("GET", "/v1/stats")
            return json.loads(conn.getresponse().read())["cluster_size"]
        except (OSError, http.client.HTTPException, ValueError, KeyError):
            return None
        finally:
            conn.close()


def main():
    targets = [Target(spec) for spec in os.environ["TARGETS"].split(",")]
    for target in targets:
        threading.Thread(target=target.run, daemon=True).start()

    offered = " + ".join(str(t.rate) for t in targets)
    total = sum(t.rate for t in targets)
    print(f"limit {LIMIT}/s on one key across the fleet; offered {offered} = {total}/s\n")
    names = "".join(f"  {t.host:>6}" for t in targets)
    print(f"{'t':>5}{names}  {'fleet':>6}  {'nodes':>5}")

    t = 0
    next_second = time.monotonic() + 1
    while True:
        time.sleep(max(0.0, next_second - time.monotonic()))
        next_second += 1
        t += 1
        row, fleet = "", 0
        for target in targets:
            allowed = target.take()
            fleet += allowed
            row += f"  {allowed if target.up else '-':>6}"
        # As the first sidecar up sees it: one that is leaving still
        # counts itself while it drains.
        sizes = (target.cluster_size() for target in targets if target.up)
        nodes = next((s for s in sizes if s is not None), "-")
        print(f"{t:>4}s{row}  {fleet:>6}  {nodes:>5}")


if __name__ == "__main__":
    main()
