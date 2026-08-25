// Seeded RNG with labeled substreams (docs/harness.md §2). Substreams keep one
// component's extra draws from shifting every other component's sequence —
// without that, any code change invalidates the failing-seed corpus.

const MASK64 = (1n << 64n) - 1n;

function splitmix64(state: bigint): { state: bigint; value: bigint } {
  let s = (state + 0x9e3779b97f4a7c15n) & MASK64;
  let z = s;
  z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK64;
  z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & MASK64;
  z = z ^ (z >> 31n);
  return { state: s, value: z };
}

function fnv1a64(s: string): bigint {
  let h = 0xcbf29ce484222325n;
  for (let i = 0; i < s.length; i++) {
    h = h ^ BigInt(s.charCodeAt(i));
    h = (h * 0x100000001b3n) & MASK64;
  }
  return h;
}

export class SeededRng {
  private state: bigint;

  constructor(seed: number | bigint) {
    this.state = BigInt(seed) & MASK64;
  }

  nextU64(): bigint {
    const r = splitmix64(this.state);
    this.state = r.state;
    return r.value;
  }

  // [0, 1) with 53 bits of precision — drop-in for opts.rng
  next(): number {
    return Number(this.nextU64() >> 11n) / 2 ** 53;
  }

  int(maxExclusive: number): number {
    return Math.floor(this.next() * maxExclusive);
  }

  substream(label: string): SeededRng {
    return new SeededRng(this.state ^ fnv1a64(label));
  }

  fn(): () => number {
    return () => this.next();
  }
}
