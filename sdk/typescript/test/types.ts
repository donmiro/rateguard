// Compiled, never run: the types say what the code does.
import { Client, SidecarError, type Decision, type KeyStats, type Stats } from "../index.js";

const limiter = new Client({ url: "http://127.0.0.1:7947", timeoutMs: 20, onError: "deny", adminTimeoutMs: 500 });
const decision: Decision = await limiter.check("tenant:42", { class: "pro" });
const wait: number = decision.retryAfterMs;
const why: string | undefined = decision.error;
// @ts-expect-error: onError is "allow" or "deny"
new Client({ onError: "maybe" });

const stats: Stats = await limiter.stats();
const named: string | undefined = stats.most_denied[0]?.key;
const one: KeyStats | null = await limiter.keyStats("tenant:42", { class: "pro" });
await limiter.setClasses({ pro: { per_minute: 60000, burst: 50 } });
await limiter.setLimit({ per_second: 100 });
await limiter.setKeyLimits([
  { class: "pro", key: "tenant:42", limit: { per_hour: 5000 } },
  { key: "tenant:7", as_class: "pro" },
]);
await limiter.setShadows({ "": { per_second: 10 }, pro: null });
await limiter.setDryRun(true);
// @ts-expect-error: a limit has one period
await limiter.setLimit({ per_second: 1, per_minute: 60 });
// @ts-expect-error: an exception has a limit or a class to follow, not both
await limiter.setKeyLimits([{ key: "k", limit: { per_second: 1 }, as_class: "pro" }]);
const status: number | undefined = new SidecarError("refused", 400).status;
export { wait, why, named, one, status };
