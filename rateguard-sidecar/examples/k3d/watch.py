"""What the whole fleet does, from inside the cluster, for watch.yaml.

Every second, lists the pods labelled SELECTOR through the Kubernetes API,
reads each one's /metrics on PORT at once, and prints what the fleet was
offered and admitted a second over the last WINDOW seconds, then what each
pod admitted.

Exact over a window, not an average of each pod's own rate: every pass
records what each counter grew since the pass before, a new pod's from 0,
as its counters start with it, and a rate is the growth over the window's
passes divided by the time since the pass before them. The API lists a pod
as soon as it has an address; the headless service's DNS answer is cached
for seconds.
"""

import collections
import concurrent.futures
import json
import os
import re
import signal
import ssl
import sys
import time
import urllib.parse
import urllib.request

SELECTOR = os.environ.get("SELECTOR", "app=api")
PORT = int(os.environ.get("PORT", "9090"))
WINDOW = float(os.environ.get("WINDOW", "2"))

ACCOUNT = "/var/run/secrets/kubernetes.io/serviceaccount"
CHECKS = re.compile(r'^rateguard_checks_total\{[^}]*decision="(allow|deny)"[^}]*\} (\S+)$')


def pods():
    """{name: address} of the pods with an address, terminating ones too;
    None if the API does not answer."""
    with open(f"{ACCOUNT}/namespace") as f:
        namespace = f.read().strip()
    with open(f"{ACCOUNT}/token") as f:
        token = f.read().strip()
    query = urllib.parse.urlencode({"labelSelector": SELECTOR})
    request = urllib.request.Request(
        f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/pods?{query}",
        headers={"Authorization": f"Bearer {token}"},
    )
    context = ssl.create_default_context(cafile=f"{ACCOUNT}/ca.crt")
    try:
        with urllib.request.urlopen(request, context=context, timeout=0.5) as r:
            items = json.load(r)["items"]
    except OSError:
        return None
    return {
        p["metadata"]["name"]: p["status"]["podIP"] for p in items if p["status"].get("podIP")
    }


def scrape(address):
    """(allowed, denied) so far, or None if it does not answer."""
    try:
        with urllib.request.urlopen(f"http://{address}:{PORT}/metrics", timeout=0.3) as r:
            text = r.read().decode()
    except OSError:
        return None
    counts = {"allow": 0.0, "deny": 0.0}
    for line in text.splitlines():
        if m := CHECKS.match(line):
            counts[m[1]] = float(m[2])
    return counts["allow"], counts["deny"]


def main():
    # As PID 1 of a container, Python has no handler for SIGTERM.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=32)
    last = {}  # pod name -> (allowed, denied) at the pass before
    # (time, {pod name: (admitted, checked) since the pass before}); the
    # first is the window's base, its growth not counted.
    passes = collections.deque()
    shown = None
    print("one key, a limit of 1000/s across the fleet; each pod offers 600/s\n")
    start = time.monotonic()
    next_second = start + 1
    while True:
        time.sleep(max(0.0, next_second - time.monotonic()))
        next_second += 1
        listed = pods()
        if listed is None:
            continue
        now = time.monotonic()
        grown = {}
        for name, sample in zip(listed, pool.map(scrape, listed.values())):
            if sample is None:
                continue
            allowed0, denied0 = last.get(name, (0.0, 0.0))
            last[name] = sample
            allowed, denied = sample
            grown[name] = (allowed - allowed0, allowed + denied - allowed0 - denied0)
        for name in set(last) - set(listed):
            del last[name]
        passes.append((now, grown))
        while len(passes) > 2 and now - passes[1][0] >= WINDOW:
            passes.popleft()
        if len(passes) < 2:
            continue
        span = now - passes[0][0]
        window = [g for _, g in list(passes)[1:]]
        admitted = sum(a for g in window for a, _ in g.values()) / span
        offered = sum(c for g in window for _, c in g.values()) / span
        # Each pod that answers, the ones terminating too.
        names = sorted(n for n in listed if n in last)
        each = [sum(g.get(n, (0, 0))[0] for g in window) / span for n in names]
        if names != shown:
            shown = names
            header = " ".join(f"{n[-5:]:>5}" for n in names)
            print(f"{'t':>5}  {'pods':>4}  {'offered':>9}  {'admitted':>9}   {header}")
        print(
            f"{now - start:>4.0f}s  {len(names):>4}  {offered:>7.0f}/s  {admitted:>7.0f}/s   "
            + " ".join(f"{e:>5.0f}" for e in each)
        )


if __name__ == "__main__":
    main()
