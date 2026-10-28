// A client for rateguard-sidecar: one global rate limit across a fleet,
// from Node.js. Types in index.d.ts.
//
// No dependencies: Node's own fetch, which keeps connections alive. If the
// sidecar cannot be reached in time, or answers with anything but a
// decision, the request is allowed (onError "allow", the default) and the
// decision carries the error: a broken limiter should not take the service
// down with it. onError "deny" denies instead.
//
// The rest of the sidecar's API is here too, off the request path: stats
// and keyStats read, and setClasses, setLimit, setKeyLimits, setShadows and
// setDryRun change limits on a sidecar started with --admin. They reject
// with a SidecarError when the sidecar does not carry them out.

const DEFAULT_URL = "http://127.0.0.1:7947";

// A call other than check that the sidecar did not carry out: status is the
// HTTP status it answered, undefined when it did not answer.
export class SidecarError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "SidecarError";
    this.status = status;
  }
}

export class Client {
  #base;
  #url;
  #timeoutMs;
  #adminTimeoutMs;
  #onError;

  constructor({ url, timeoutMs = 50, onError = "allow", adminTimeoutMs = 1000 } = {}) {
    if (onError !== "allow" && onError !== "deny") {
      throw new TypeError(`onError is "allow" or "deny", not ${JSON.stringify(onError)}`);
    }
    const base = url || process.env.RATEGUARD_SIDECAR_URL || DEFAULT_URL;
    // A path in the URL is kept, as a proxy in front of the sidecar may need.
    this.#base = base.replace(/\/+$/, "");
    this.#url = new URL(`${this.#base}/v1/check`).href;
    this.#timeoutMs = timeoutMs;
    this.#adminTimeoutMs = adminTimeoutMs;
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

  stats() {
    return this.#call("GET", "/v1/stats");
  }

  // null while the sidecar keeps no state for the key.
  keyStats(key, { class: cls } = {}) {
    const query = new URLSearchParams(cls === undefined ? { key } : { key, class: cls });
    return this.#call("GET", `/v1/keys?${query}`, undefined, { missingIsNull: true });
  }

  async setClasses(classes) {
    await this.#call("PUT", "/v1/classes", classes);
  }

  async setLimit(limit) {
    await this.#call("PUT", "/v1/limit", limit);
  }

  async setKeyLimits(keyLimits) {
    await this.#call("PUT", "/v1/key_limits", keyLimits);
  }

  async setShadows(shadows) {
    await this.#call("PUT", "/v1/shadows", shadows);
  }

  async setDryRun(on) {
    await this.#call("PUT", "/v1/dry_run", { on });
  }

  async #call(method, path, body, { missingIsNull = false } = {}) {
    let response, text;
    try {
      response = await fetch(new URL(this.#base + path).href, {
        method,
        headers: body === undefined ? {} : { "content-type": "application/json" },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(this.#adminTimeoutMs),
      });
      text = await response.text();
    } catch (error) {
      throw new SidecarError(`the sidecar did not answer: ${error.message}`, undefined);
    }
    if (response.status === 404 && missingIsNull) return null;
    if (response.status === 404 && method === "PUT") {
      throw new SidecarError("the sidecar has no admin routes: start it with --admin", 404);
    }
    if (response.status !== 200 && response.status !== 204) {
      throw new SidecarError(`the sidecar answered ${response.status}: ${reason(text)}`, response.status);
    }
    if (response.status === 204) return undefined;
    try {
      return JSON.parse(text);
    } catch {
      throw new SidecarError("not JSON from the sidecar", response.status);
    }
  }
}

// The sidecar's {"error": ...}, or the body as it is.
function reason(text) {
  try {
    const error = JSON.parse(text)?.error;
    if (typeof error === "string") return error;
  } catch {}
  return text || "no reason given";
}
