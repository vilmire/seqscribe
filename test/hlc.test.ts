import { describe, expect, it } from "vitest";
import {
  HLC_C_LIMIT,
  hlcCompare,
  isOverEpsilon,
  merge,
  orderCompare,
  SeqscribeError,
  stamp,
} from "../src/index.js";

describe("HLC stamp (§3)", () => {
  it("advances l to pt and resets c", () => {
    const r = stamp({ l: 100, c: 5 }, 200);
    expect(r.hlc).toEqual({ l: 200, c: 0 });
  });

  it("increments c when pt lags", () => {
    const r = stamp({ l: 200, c: 5 }, 100);
    expect(r.hlc).toEqual({ l: 200, c: 6 });
  });

  it("is strictly monotonic across stamps", () => {
    let state = { l: 0, c: 0 };
    let prev = { l: -1, c: 0 };
    for (const pt of [10, 10, 10, 5, 20, 20]) {
      const r = stamp(state, pt);
      state = r.state;
      expect(hlcCompare(prev, r.hlc)).toBeLessThan(0);
      prev = r.hlc;
    }
  });

  it("throws ERR_ENTRY_ENCODING at c overflow", () => {
    expect(() => stamp({ l: 100, c: HLC_C_LIMIT - 1 }, 50)).toThrowError(SeqscribeError);
  });
});

describe("HLC merge (§3)", () => {
  it("takes max of three sources", () => {
    expect(merge({ l: 100, c: 2 }, { l: 100, c: 7 }, 50)).toEqual({ l: 100, c: 8 });
    expect(merge({ l: 100, c: 2 }, { l: 90, c: 7 }, 50)).toEqual({ l: 100, c: 3 });
    expect(merge({ l: 80, c: 2 }, { l: 90, c: 7 }, 50)).toEqual({ l: 90, c: 8 });
    expect(merge({ l: 80, c: 2 }, { l: 90, c: 7 }, 200)).toEqual({ l: 200, c: 0 });
  });

  it("carries deterministically instead of overflowing c", () => {
    const r = merge({ l: 100, c: HLC_C_LIMIT - 1 }, { l: 100, c: HLC_C_LIMIT - 1 }, 50);
    expect(r).toEqual({ l: 101, c: 0 });
  });

  it("over-ε detection", () => {
    expect(isOverEpsilon({ l: 1000, c: 0 }, 500, 300)).toBe(true);
    expect(isOverEpsilon({ l: 799, c: 0 }, 500, 300)).toBe(false);
  });
});

describe("total order (§1)", () => {
  it("orders lexicographically over (l, c, writer, seq)", () => {
    const base = { l: 10, c: 1, writer: "b", seq: 5 };
    expect(orderCompare(base, { ...base })).toBe(0);
    expect(orderCompare({ ...base, l: 9 }, base)).toBeLessThan(0);
    expect(orderCompare({ ...base, c: 0 }, base)).toBeLessThan(0);
    expect(orderCompare({ ...base, writer: "a" }, base)).toBeLessThan(0);
    expect(orderCompare({ ...base, seq: 4 }, base)).toBeLessThan(0);
    expect(orderCompare({ ...base, writer: "B" }, base)).toBeLessThan(0); // bytewise: "B" < "b"
  });
});
