// Reference socket transport (DESIGN §8, SPEC §7 non-goals: "reference
// only"). Wraps an ALREADY-CONNECTED socket into the §14 Channel — discovery,
// auth, and reconnection remain the host's. Works with the browser/Node global
// WebSocket, `ws` package sockets, AND RTCDataChannel (proposals-v3.5 P4):
// readiness is judged by `isOpen()` when present, else readyState of either
// the numeric WebSocket form (1) or the string RTCDataChannel form ("open").
// A wrapper with neither — or without addEventListener — needs its own adapter.

import type { Channel } from "./types.js";

export interface WebSocketLike {
  readyState?: number | string;
  isOpen?: () => boolean;
  send(data: string): void;
  close(): void;
  addEventListener(type: "message", cb: (ev: { data: unknown }) => void): void;
  addEventListener(type: "close", cb: () => void): void;
  addEventListener(type: "open", cb: () => void): void;
}

function socketOpen(ws: WebSocketLike): boolean {
  if (typeof ws.isOpen === "function") return ws.isOpen();
  return ws.readyState === 1 || ws.readyState === "open";
}

// pre-open frames are bounded (P4): a transport that never opens must not
// grow memory without limit — dropped frames recover via WANT/anti-entropy
const PRE_OPEN_CAP = 1024;

const textDec = new (globalThis as unknown as {
  TextDecoder: new () => { decode(b: Uint8Array): string };
}).TextDecoder();

export function webSocketChannel(ws: WebSocketLike): Channel {
  let messageCb: ((m: string) => void) | null = null;
  let closeCb: (() => void) | null = null;
  let closed = false;
  const preOpen: string[] = [];
  const flushPreOpen = () => {
    // in order, ahead of any frame that triggered the flush
    for (const m of preOpen.splice(0)) ws.send(m);
  };

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
    flushPreOpen();
  });

  return {
    send(msg: string): void {
      if (closed) return;
      if (socketOpen(ws)) {
        flushPreOpen(); // covers transports whose "open" event fired before wiring
        ws.send(msg);
      } else if (preOpen.length < PRE_OPEN_CAP) {
        preOpen.push(msg);
      }
      // beyond the cap: drop — the protocol's retry/anti-entropy paths recover
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

// discoverability alias: the adapter handles RTCDataChannel (and isOpen()
// wrappers) natively since proposals-v3.5 P4 — same implementation
export const dataChannelChannel = webSocketChannel;
