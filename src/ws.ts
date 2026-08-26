// Reference WebSocket transport (DESIGN §8, SPEC §7 non-goals: "reference
// only"). Wraps an ALREADY-CONNECTED socket into the §14 Channel — discovery,
// auth, and reconnection remain the host's. Works with the browser/Node global
// WebSocket and with `ws` package sockets (both speak addEventListener).

import type { Channel } from "./types.js";

export interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(): void;
  addEventListener(type: "message", cb: (ev: { data: unknown }) => void): void;
  addEventListener(type: "close", cb: () => void): void;
  addEventListener(type: "open", cb: () => void): void;
}

const OPEN = 1;

const textDec = new (globalThis as unknown as {
  TextDecoder: new () => { decode(b: Uint8Array): string };
}).TextDecoder();

export function webSocketChannel(ws: WebSocketLike): Channel {
  let messageCb: ((m: string) => void) | null = null;
  let closeCb: (() => void) | null = null;
  let closed = false;
  const preOpen: string[] = [];

  ws.addEventListener("message", (ev) => {
    if (!messageCb) return;
    const d = ev.data;
    if (typeof d === "string") messageCb(d);
    else if (d instanceof Uint8Array) messageCb(textDec.decode(d));
    else if (d instanceof ArrayBuffer) messageCb(textDec.decode(new Uint8Array(d)));
  });
  ws.addEventListener("close", () => {
    if (closed) return;
    closed = true;
    closeCb?.();
  });
  // convenience: frames sent before the socket finishes opening are buffered —
  // the contract hands over a connected channel, but hosts race dial vs attach
  ws.addEventListener("open", () => {
    for (const m of preOpen.splice(0)) ws.send(m);
  });

  return {
    send(msg: string): void {
      if (closed) return;
      if (ws.readyState === OPEN) ws.send(msg);
      else preOpen.push(msg);
    },
    onMessage(cb): void {
      messageCb = cb;
    },
    onClose(cb): void {
      closeCb = cb;
    },
    close(): void {
      if (closed) return;
      closed = true;
      try {
        ws.close();
      } catch {
        // already closing
      }
      closeCb?.();
    },
  };
}
