/** What a check decides. */
export interface Decision {
  /** Whether to serve the request. */
  allowed: boolean;
  /** When denied, how long until a retry may be admitted, in milliseconds. */
  retryAfterMs: number;
  /**
   * Set when the sidecar could not answer: the decision then follows
   * `onError`, except for a key over the sidecar's 64 KiB limit (a 413),
   * which is always denied.
   */
  error?: string;
}

export interface ClientOptions {
  /** The sidecar: `$RATEGUARD_SIDECAR_URL`, or `http://127.0.0.1:7947`. */
  url?: string;
  /**
   * How long to wait for an answer: 50 ms by default, the sidecar answering
   * in a fraction of a millisecond on loopback.
   */
  timeoutMs?: number;
  /** Allow or deny the request when the sidecar cannot answer: `"allow"`. */
  onError?: "allow" | "deny";
}

/** Checks keys through a rateguard-sidecar on this machine. */
export class Client {
  constructor(options?: ClientOptions);
  /** Admits or denies one request for `key`, in `class` or the default one. */
  check(key: string, options?: { class?: string }): Promise<Decision>;
}
