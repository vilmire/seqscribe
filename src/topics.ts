// SPEC §14 — TopicPolicy validation, schemaHash, per-process immutability.

import { assertTopic } from "./codec.js";
import { jcs, normalizedPolicy, topicSchemaHashOf } from "./encoding.js";
import { misuse, SeqscribeError } from "./errors.js";
import type { AuthorityHooks, JsonValue, Topic, TopicPolicy } from "./types.js";

export interface TopicEntry {
  policy: TopicPolicy;
  schemaHash: string;
}

const CONFLICT_POLICIES = new Set(["lww", "fww", "resolver", "owned"]);
// §11.7: exact keys or prefix globs — a literal prefix followed by ".*", no other wildcards.
const OVERRIDE_GLOB_RE = /^[^*]+\.\*$/;

function usesOwned(p: TopicPolicy): boolean {
  if (p.conflict?.default === "owned") return true;
  return Object.values(p.conflict?.overrides ?? {}).includes("owned");
}

export function validatePolicy(topic: Topic, p: TopicPolicy, authority?: AuthorityHooks): void {
  assertTopic(topic);
  if (p.kind !== "append" && p.kind !== "register") throw misuse(`bad kind for ${topic}`);

  const mode = p.retention.mode;
  if (mode !== "full" && mode !== "ring" && mode !== "none")
    throw misuse(`bad retention mode for ${topic}`);
  if (mode === "ring") {
    const size = (p.retention as { size: number }).size;
    if (!Number.isSafeInteger(size) || size < 1) throw misuse(`bad ring size for ${topic}`);
  }
  if ((mode === "ring" || mode === "none") && (p.replication !== "subscribe-only" || p.kind !== "append"))
    throw misuse(`${mode} retention requires subscribe-only replication and kind "append" (${topic})`);
  if (p.kind === "register" && mode !== "full")
    throw misuse(`register topics require retention "full" (${topic})`);

  if (p.replication !== "full-sync" && p.replication !== "subscribe-only")
    throw misuse(`bad replication for ${topic}`);
  if (p.access !== "content" && p.access !== "metadata") throw misuse(`bad access for ${topic}`);

  if (p.conflict !== undefined) {
    if (p.kind !== "register") throw misuse(`conflict policy on non-register topic ${topic}`);
    if (p.conflict.default !== undefined && !CONFLICT_POLICIES.has(p.conflict.default))
      throw misuse(`bad conflict.default for ${topic}`);
    for (const [pattern, policy] of Object.entries(p.conflict.overrides ?? {})) {
      if (!CONFLICT_POLICIES.has(policy)) throw misuse(`bad override policy for ${topic}`);
      if (pattern.includes("*") && !OVERRIDE_GLOB_RE.test(pattern))
        throw misuse(`bad override pattern "${pattern}" for ${topic} (exact key or "prefix.*" only)`);
    }
  }

  if (p.finalityAuthority !== undefined && !authority?.verifyFinality)
    throw misuse(`${topic} sets finalityAuthority but authority.verifyFinality is absent`);
  if (usesOwned(p) && (!authority?.verifyTakeover || !authority?.verifyWriterDirective))
    throw misuse(`${topic} uses "owned" but verifyTakeover/verifyWriterDirective are absent`);

  if (p.hintKeys !== undefined && p.hintKeys !== "plain" && p.hintKeys !== "hash")
    throw misuse(`bad hintKeys for ${topic}`);
  if (p.flushThrottleMs !== undefined && (!Number.isSafeInteger(p.flushThrottleMs) || p.flushThrottleMs < 0))
    throw misuse(`bad flushThrottleMs for ${topic}`);
}

// §11.7 conflict-policy resolution: exact match > longest matching prefix > default.
export function conflictPolicyFor(
  p: TopicPolicy,
  key: string,
): "lww" | "fww" | "resolver" | "owned" {
  const overrides = p.conflict?.overrides ?? {};
  const exact = overrides[key];
  if (exact !== undefined) return exact;
  let best: { len: number; policy: "lww" | "fww" | "resolver" | "owned" } | undefined;
  for (const [pattern, policy] of Object.entries(overrides)) {
    if (!pattern.endsWith(".*")) continue;
    const prefix = pattern.slice(0, -1); // keep the trailing "." — "a.*" matches "a.b", not "ab"
    if (key.startsWith(prefix) && (best === undefined || prefix.length > best.len))
      best = { len: prefix.length, policy };
  }
  return best?.policy ?? p.conflict?.default ?? "lww";
}

export class TopicRegistry {
  private readonly topics = new Map<Topic, TopicEntry>();

  define(topic: Topic, policy: TopicPolicy, authority?: AuthorityHooks): TopicEntry {
    validatePolicy(topic, policy, authority);
    const existing = this.topics.get(topic);
    if (existing) {
      // Policies are immutable per process (§14); JCS equality over the full
      // policy subsumes the schemaHash rule.
      if (jcs(policy as unknown as JsonValue) !== jcs(existing.policy as unknown as JsonValue))
        throw misuse(`re-defineTopic with a changed policy: ${topic}`);
      return existing;
    }
    const entry: TopicEntry = { policy, schemaHash: topicSchemaHashOf(policy) };
    this.topics.set(topic, entry);
    return entry;
  }

  get(topic: Topic): TopicEntry {
    const e = this.topics.get(topic);
    if (!e) throw new SeqscribeError("ERR_UNKNOWN_TOPIC", topic);
    return e;
  }

  has(topic: Topic): boolean {
    return this.topics.has(topic);
  }

  list(): Topic[] {
    return [...this.topics.keys()];
  }

  normalized(topic: Topic): JsonValue {
    return normalizedPolicy(this.get(topic).policy);
  }
}
