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
  /** How long to wait for every call but `check`: 1000 ms by default. */
  adminTimeoutMs?: number;
}

/**
 * A count of requests per second, minute or hour, exactly one of them, and
 * a burst: by default a twentieth of the count.
 */
export type Limit =
  | { per_second: number; per_minute?: never; per_hour?: never; burst?: number }
  | { per_minute: number; per_second?: never; per_hour?: never; burst?: number }
  | { per_hour: number; per_second?: never; per_minute?: never; burst?: number };

/**
 * One key's exception: its own `limit`, or `as_class`, the class whose
 * limit it follows. `class` is the one the service checks the key in, the
 * default if left out.
 */
export type KeyLimit =
  | { class?: string; key: string; limit: Limit; as_class?: never }
  | { class?: string; key: string; as_class: string; limit?: never };

/** One of the keys denied most lately, its rates over about ten seconds. */
export interface DeniedKey {
  /** A string: a JSON number would lose the last digits of the hash. */
  key_hash: string;
  denied_per_sec: number;
  requests_per_sec: number;
  shadow_denied_per_sec: number;
  /** The key's name and class, while the sidecar knows them. */
  key?: string;
  class?: string;
  key_truncated?: boolean;
}

/** What `/v1/stats` tells; the sidecar's README says what each field means. */
export interface Stats {
  admitted: number;
  denied: number;
  overflowed: number;
  unknown_class: number;
  cluster_size: number;
  counted_size: number;
  suspects: number;
  tracked_keys: number;
  hot_keys: number;
  joining: boolean;
  learning: boolean;
  without_quorum: boolean;
  members_full: boolean;
  since_last_round_ms: number | null;
  running: boolean;
  dry_run: boolean;
  shadow_denied: number;
  most_denied: DeniedKey[];
  most_shadow_denied: DeniedKey[];
  gossip_auth: "off" | "transitional" | "required";
  rejected_datagrams: {
    unsigned: number;
    bad_tag: number;
    malformed: number;
    mismatched_records: number;
  };
  identity_conflicts: number;
  members_refused: number;
}

/** One key on this sidecar. */
export interface KeyStats {
  requests_per_sec: number;
  denied_per_sec: number;
  shadow_denied_per_sec: number;
  /** The rate this sidecar admits the key at now. */
  share: number;
  /** The limit the key is held to, its own or its class's. */
  limit: Limit;
  shadow: Limit | null;
}

/**
 * A call other than `check` that the sidecar did not carry out: `status` is
 * the HTTP status it answered, undefined when it did not answer.
 */
export class SidecarError extends Error {
  constructor(message: string, status?: number);
  status: number | undefined;
}

/** Checks keys through a rateguard-sidecar on this machine. */
export class Client {
  constructor(options?: ClientOptions);
  /** Admits or denies one request for `key`, in `class` or the default one. */
  check(key: string, options?: { class?: string }): Promise<Decision>;
  /** What the sidecar admitted and denied, and why. */
  stats(): Promise<Stats>;
  /** One key, in `class` or the default one; null while it is not tracked. */
  keyStats(key: string, options?: { class?: string }): Promise<KeyStats | null>;
  /** Replaces every named class. Needs `--admin`, as every call below. */
  setClasses(classes: Record<string, Limit>): Promise<void>;
  /** Sets the default class's limit. */
  setLimit(limit: Limit): Promise<void>;
  /** Replaces every key's exception. */
  setKeyLimits(keyLimits: KeyLimit[]): Promise<void>;
  /**
   * Replaces every shadow limit, by class, `""` for the default: a limit
   * that denies nothing, and counts what it would deny.
   */
  setShadows(shadows: Record<string, Limit | null>): Promise<void>;
  /** Admits everything while on, and counts what would be denied. */
  setDryRun(on: boolean): Promise<void>;
}
