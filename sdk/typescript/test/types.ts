// Compiled, never run: the types say what the code does.
import { Client, type Decision } from "../index.js";

const limiter = new Client({ url: "http://127.0.0.1:7947", timeoutMs: 20, onError: "deny" });
const decision: Decision = await limiter.check("tenant:42", { class: "pro" });
const wait: number = decision.retryAfterMs;
const why: string | undefined = decision.error;
// @ts-expect-error: onError is "allow" or "deny"
new Client({ onError: "maybe" });
export { wait, why };
