import type { ErrCode } from "./types.js";

export class SeqscribeError extends Error {
  code: ErrCode;
  constructor(code: ErrCode, message?: string) {
    super(message ? `${code}: ${message}` : code);
    this.name = "SeqscribeError";
    this.code = code;
  }
}

export function misuse(message: string): SeqscribeError {
  return new SeqscribeError("ERR_MISUSE", message);
}
