"""The calls off the request path, stats and the admin routes: against a
fake sidecar, and against a real one."""

import json
import os
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from rateguard_client import Client, SidecarError


class FakeAdmin:
    """Answers every request with `status` and `reply`, and keeps what it
    was sent: method, path, and the body as JSON."""

    def __init__(self, status=204, reply=None):
        self.seen = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                outer.seen.append((self.command, self.path, body))
                data = b"" if reply is None else json.dumps(reply).encode()
                self.send_response(status)
                if data:
                    self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_PUT = answer

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class AdminTest(unittest.TestCase):
    def fake(self, status=204, reply=None):
        sidecar = FakeAdmin(status, reply)
        self.addCleanup(sidecar.close)
        client = Client(sidecar.url)
        self.addCleanup(client.close)
        return sidecar, client

    def test_each_change_is_a_put_of_its_json(self):
        sidecar, client = self.fake()
        pro = {"per_minute": 60000, "burst": 50}
        client.set_classes({"pro": pro})
        client.set_limit({"per_second": 100})
        client.set_key_limits([{"key": "tenant:7", "as_class": "pro"}])
        client.set_shadows({"": {"per_second": 10}, "pro": None})
        client.set_dry_run(True)
        self.assertEqual(
            sidecar.seen,
            [
                ("PUT", "/v1/classes", {"pro": pro}),
                ("PUT", "/v1/limit", {"per_second": 100}),
                ("PUT", "/v1/key_limits", [{"key": "tenant:7", "as_class": "pro"}]),
                ("PUT", "/v1/shadows", {"": {"per_second": 10}, "pro": None}),
                ("PUT", "/v1/dry_run", {"on": True}),
            ],
        )

    def test_stats_are_the_sidecar_s_json(self):
        sidecar, client = self.fake(200, {"admitted": 5, "most_denied": []})
        self.assertEqual(client.stats(), {"admitted": 5, "most_denied": []})
        self.assertEqual(sidecar.seen, [("GET", "/v1/stats", None)])

    def test_key_stats_ask_for_the_key_and_its_class_encoded(self):
        sidecar, client = self.fake(200, {"share": 1.5})
        self.assertEqual(client.key_stats("tenant 42&x", cls="pro"), {"share": 1.5})
        client.key_stats("k")
        self.assertEqual(sidecar.seen[0][1], "/v1/keys?key=tenant+42%26x&class=pro")
        self.assertEqual(sidecar.seen[1][1], "/v1/keys?key=k")

    def test_a_key_the_sidecar_does_not_track_is_none(self):
        _, client = self.fake(404, {"error": "this node does not track the key"})
        self.assertIsNone(client.key_stats("k"))

    def test_a_refused_change_raises_with_the_sidecar_s_reason(self):
        _, client = self.fake(400, {"error": "a limit needs one of per_second"})
        with self.assertRaisesRegex(SidecarError, "400: a limit needs one of per_second") as caught:
            client.set_limit({"burst": 5})
        self.assertEqual(caught.exception.status, 400)

    def test_a_sidecar_without_admin_says_so(self):
        _, client = self.fake(404)
        with self.assertRaisesRegex(SidecarError, "--admin"):
            client.set_dry_run(False)

    def test_a_sidecar_that_is_gone_raises_with_no_status(self):
        client = Client("http://127.0.0.1:1", admin_timeout=0.2)
        with self.assertRaises(SidecarError) as caught:
            client.stats()
        self.assertIsNone(caught.exception.status)

    def test_a_path_in_the_url_is_kept(self):
        sidecar = FakeAdmin(200, {})
        self.addCleanup(sidecar.close)
        Client(sidecar.url + "/rateguard/").stats()
        self.assertEqual(sidecar.seen[0][1], "/rateguard/v1/stats")


def eventually(read, done):
    """What `read` returns once `done` holds of it: the sidecar's counts and
    a new key's state follow its background task, a tick behind."""
    deadline = time.monotonic() + 2
    while True:
        value = read()
        if done(value) or time.monotonic() > deadline:
            return value
        time.sleep(0.01)


@unittest.skipUnless(os.environ.get("RATEGUARD_LIVE_URL"), "RATEGUARD_LIVE_URL is not set")
class LiveAdminTest(unittest.TestCase):
    """Against a real `rateguard-sidecar --limit 2 --burst 2 --admin`: see
    sdk/live/run.sh. Each change is put back as it was."""

    def setUp(self):
        self.client = Client(os.environ["RATEGUARD_LIVE_URL"], timeout=2.0)
        self.addCleanup(self.client.close)

    def test_a_key_limit_lets_its_key_past_the_default(self):
        key = f"py-live-own-{os.getpid()}"
        self.client.set_key_limits([{"key": key, "limit": {"per_second": 1, "burst": 4}}])
        self.addCleanup(self.client.set_key_limits, [])
        decisions = [self.client.check(key).allowed for _ in range(5)]
        self.assertEqual(decisions, [True, True, True, True, False])
        stats = eventually(lambda: self.client.key_stats(key), lambda s: s is not None)
        self.assertEqual(stats["limit"], {"per_second": 1, "burst": 4})
        self.assertIsNone(stats["shadow"])
        self.assertIsNone(self.client.key_stats(f"py-live-never-{os.getpid()}"))

    def test_a_shadow_counts_what_it_would_deny(self):
        key = f"py-live-shadow-{os.getpid()}"
        before = self.client.stats()["shadow_denied"]
        self.client.set_shadows({"": {"per_second": 1, "burst": 1}})
        self.addCleanup(self.client.set_shadows, {})
        self.assertTrue(self.client.check(key).allowed)
        self.assertTrue(self.client.check(key).allowed)
        stats = eventually(self.client.stats, lambda s: s["shadow_denied"] > before)
        self.assertGreater(stats["shadow_denied"], before)

    def test_a_refused_limit_raises_and_a_good_one_is_taken(self):
        with self.assertRaises(SidecarError) as caught:
            self.client.set_limit({"per_second": 0})
        self.assertEqual(caught.exception.status, 400)
        self.client.set_limit({"per_second": 2, "burst": 2})
        self.client.set_classes({})
        self.client.set_dry_run(False)
        self.assertEqual(self.client.stats()["dry_run"], False)


if __name__ == "__main__":
    unittest.main()
