// A client for rateguard-sidecar: one global rate limit across a fleet,
// from Node.js. Types in index.d.ts.
//
// No dependencies: Node's own fetch, which keeps connections alive. If the
// sidecar cannot be reached in time, or answers with anything but a
// decision, the request is allowed (onError "allow", the default) and the
// decision carries the error: a broken limiter should not take the service
// down with it. onError "deny" denies instead.

const DEFAULT_URL = "http://127.0.0.1:7947";

export class Client {
  #url;
  #timeoutMs;
  #onError;

  constructor({ url, timeoutMs = 50, onError = "allow" } = {}) {
    if (onError !== "allow" && onError !== "deny") {
      throw new TypeError(`onError is "allow" or "deny", not ${JSON.stringify(onError)}`);
    }
    const base = url || process.env.RATEGUARD_SIDECAR_URL || DEFAULT_URL;
    // A path in the URL is kept, as a proxy in front of the sidecar may need.
    this.#url = new URL(`${base.replace(/\/+$/, "")}/v1/check`).href;
    this.#timeoutMs = timeoutMs;
    this.#onError = onError === "allow";
  }

  async check(key, { class: cls } = {}) {
    const body = cls === undefined ? { key } : { key, class: cls };
    let response;
    try {
      response = await fetch(this.#url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(this.#timeoutMs),
      });
    } catch (error) {
      return this.#failed(`the sidecar did not answer: ${error.message}`);
    }
    // The status first: an error page need not be JSON, and the status is
    // what says what to do with it. The body is cancelled unread, to free
    // the connection; that rejects if the body has errored already, the
    // timeout firing after the headers, say, and the decision stands all
    // the same.
    if (response.status === 413) {
      // The sidecar refuses a key this large on every attempt: allowing it
      // would let the key past the limit for good.
      await response.body?.cancel().catch(() => {});
      return { allowed: false, retryAfterMs: 0, error: "the key is over the sidecar's 64 KiB limit" };
    }
    if (response.status !== 200) {
      await response.body?.cancel().catch(() => {});
      return this.#failed(`the sidecar answered ${response.status}`);
    }
    let answer;
    try {
      answer = await response.json();
    } catch (error) {
      return this.#failed(`not a decision from the sidecar: ${error.message}`);
    }
    if (answer?.allowed === true) {
      return { allowed: true, retryAfterMs: 0 };
    }
    if (answer?.allowed === false && Number.isFinite(answer.retry_after_ms)) {
      return { allowed: false, retryAfterMs: answer.retry_after_ms };
    }
    return this.#failed("not a decision from the sidecar");
  }

  #failed(error) {
    return { allowed: this.#onError, retryAfterMs: 0, error };
  }
}
