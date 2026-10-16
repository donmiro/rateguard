"""The client against a fake sidecar: what it sends, what it makes of the
answers, and what it does when the sidecar is not there."""

import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from rateguard_client import Client, Decision


class FakeSidecar:
    """Answers /v1/check with what `answer` returns for the request body."""

    def __init__(self, answer, delay=0.0, status=200):
        self.seen = []
        self.peers = set()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):
                length = int(self.headers["Content-Length"])
                body = json.loads(self.rfile.read(length))
                outer.seen.append((self.path, body))
                outer.peers.add(self.client_address)
                time.sleep(delay)
                reply = json.dumps(answer(body)).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class ClientTest(unittest.TestCase):
    def client(self, *args, **kwargs):
        client = Client(*args, **kwargs)
        self.addCleanup(client.close)
        return client

    def fake(self, answer, **kwargs):
        sidecar = FakeSidecar(answer, **kwargs)
        self.addCleanup(sidecar.close)
        return sidecar

    def test_an_allowed_check(self):
        sidecar = self.fake(lambda body: {"allowed": True})
        decision = self.client(sidecar.url).check("tenant:42")
        self.assertEqual(decision, Decision(allowed=True, retry_after=0.0))
        self.assertEqual(sidecar.seen, [("/v1/check", {"key": "tenant:42"})])

    def test_a_denied_check_says_when_to_retry(self):
        sidecar = self.fake(lambda body: {"allowed": False, "retry_after_ms": 1500})
        decision = self.client(sidecar.url).check("tenant:42")
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.retry_after, 1.5)

    def test_a_check_in_a_class_sends_the_class(self):
        sidecar = self.fake(lambda body: {"allowed": True})
        self.client(sidecar.url).check("tenant:42", cls="pro")
        self.assertEqual(sidecar.seen[0][1], {"key": "tenant:42", "class": "pro"})

    def test_one_connection_serves_many_checks(self):
        sidecar = self.fake(lambda body: {"allowed": True})
        with Client(sidecar.url) as client:
            for _ in range(50):
                self.assertTrue(client.check("k").allowed)
        self.assertEqual(len(sidecar.seen), 50)
        self.assertEqual(len(sidecar.peers), 1, "one connection, kept alive")

    def test_a_slow_sidecar_is_given_up_on_and_the_request_allowed(self):
        sidecar = self.fake(lambda body: {"allowed": False}, delay=0.5)
        started = time.monotonic()
        decision = self.client(sidecar.url, timeout=0.05).check("k")
        self.assertTrue(decision.allowed, "on_error allow by default")
        self.assertLess(time.monotonic() - started, 0.4)

    def test_on_error_deny_denies_when_the_sidecar_is_gone(self):
        client = self.client("http://127.0.0.1:1", timeout=0.05, on_error="deny")
        decision = client.check("k")
        self.assertFalse(decision.allowed)

    def test_a_sidecar_that_answers_badly_counts_as_an_error(self):
        sidecar = self.fake(lambda body: {"error": "no key"}, status=400)
        self.assertTrue(self.client(sidecar.url).check("k").allowed)
        self.assertFalse(self.client(sidecar.url, on_error="deny").check("k").allowed)

    def test_an_unknown_on_error_is_refused(self):
        with self.assertRaises(ValueError):
            Client(on_error="maybe")

    def test_the_url_comes_from_the_environment_by_default(self):
        sidecar = self.fake(lambda body: {"allowed": True})
        import os

        os.environ["RATEGUARD_SIDECAR_URL"] = sidecar.url
        self.addCleanup(os.environ.pop, "RATEGUARD_SIDECAR_URL")
        self.assertTrue(self.client().check("k").allowed)
        self.assertEqual(len(sidecar.seen), 1)


class PathTest(unittest.TestCase):
    def test_a_path_in_the_url_is_kept(self):
        sidecar = FakeSidecar(lambda body: {"allowed": True})
        self.addCleanup(sidecar.close)
        with Client(sidecar.url + "/rateguard/") as client:
            client.check("k")
        self.assertEqual(sidecar.seen[0][0], "/rateguard/v1/check")

    def test_a_bad_url_from_the_environment_is_named(self):
        import os

        os.environ["RATEGUARD_SIDECAR_URL"] = "127.0.0.1:7947"
        self.addCleanup(os.environ.pop, "RATEGUARD_SIDECAR_URL")
        with self.assertRaisesRegex(ValueError, "127.0.0.1:7947"):
            Client()


class ThreadsTest(unittest.TestCase):
    def test_close_closes_every_thread_s_connection(self):
        sidecar = FakeSidecar(lambda body: {"allowed": True})
        self.addCleanup(sidecar.close)
        client = Client(sidecar.url)
        threads = [threading.Thread(target=client.check, args=("k",)) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        client.check("k")
        self.assertEqual(len(sidecar.peers), 5)
        client.close()
        self.assertEqual(client._open(), 0)


if __name__ == "__main__":
    unittest.main()
