import type { Constants } from "./types.js";

// SPEC §16 defaults. Day-denominated rows are milliseconds.
export const DEFAULT_CONSTANTS: Constants = {
  MAX_ENTRY_BYTES: 65_536,
  MAX_FRAME_BYTES: 262_144,
  MAX_ROW_BYTES: 65_536,
  INFLIGHT_CREDITS: 4,
  CHANNEL_STALL_MS: 30_000,
  SEND_QUEUE_CAP: 64,
  CONTROL_RETRY_MS: 5_000,
  PENDING_CAP: 1_024,
  SUB_DELTA_RETAIN: 256,
  EAGER_PUSH_K: 4,
  RELAY_FANOUT: 4,
  RELAY_WINDOW_MS: 30_000,
  ANTI_ENTROPY_MS: 300_000,
  BEACON_DEBOUNCE_MS: 5_000,
  HLC_EPSILON_MS: 300_000,
  CLOCK_WARN_MS: 60_000,
  CHECKPOINT_EVERY: 10_000,
  RING_DEFAULT: 500,
  GROUP_COMMIT_MS: 20,
  GROUP_COMMIT_N: 64,
  HELLO_TIMEOUT_MS: 5_000,
  FINALITY_WINDOW_MS: 2_592_000_000, // 30 d
  RETIRE_GRACE_MS: 2_592_000_000, // 30 d
  REQUEST_TTL_MS: 2_592_000_000, // 30 d
  TOMBSTONE_RETAIN_MS: 34_560_000_000, // 400 d
  durability: "normal",
};

export function resolveConstants(overrides?: Partial<Constants>): Constants {
  return { ...DEFAULT_CONSTANTS, ...overrides };
}
