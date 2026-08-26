// Main thread: rendering + a jank monitor. All seqscribe work is in the worker.
const $ = (id) => document.getElementById(id);
const worker = new Worker("./dashboard-worker.js", { type: "module" });

// main-thread blocking monitor via the Long Tasks API: unlike rAF/interval
// drift, it is immune to background-tab timer throttling and reports ONLY
// genuine >50 ms tasks on this thread. The worker doing all seqscribe work is
// exactly what keeps this list empty.
let longTasks = 0;
let maxTaskMs = 0;
try {
  new PerformanceObserver((list) => {
    for (const e of list.getEntries()) {
      longTasks++;
      maxTaskMs = Math.max(maxTaskMs, e.duration);
      $("jank").textContent = `main-thread long tasks: ${longTasks} (max ${maxTaskMs.toFixed(0)}ms)`;
    }
  }).observe({ entryTypes: ["longtask"] });
  $("jank").textContent = "main-thread long tasks: 0";
} catch {
  $("jank").textContent = "longtask API unavailable";
}
const spin = $("spinner");
(function loop(now) {
  spin.style.transform = `rotate(${(now / 4) % 360}deg)`;
  requestAnimationFrame(loop);
})(performance.now());

let state = null;
let heavy = null;
worker.onmessage = (ev) => {
  const m = ev.data;
  if (m.type === "error") {
    $("status").textContent = "ERROR";
    $("result").textContent = JSON.stringify({ ok: false, error: m.error });
    return;
  }
  if (m.type === "heavy") heavy = m;
  if (m.type === "state") {
    state = m;
    $("status").textContent =
      `${m.vfs} · ${m.writerId} · mesh contig ${m.meshContig}` +
      (m.resumedFromSeq > 0 ? ` · resumed from ${m.resumedFromSeq}` : " · fresh");
    $("ledger").textContent = m.ledgerRows.map((r) => `${r.task}  ${r.status.padEnd(12)} ${r.machine}`).join("\n");
    $("fleet").textContent = m.fleet.map((f) => `${f.machine}  cpu=${String(f.payload.cpu).padStart(3)}%`).join("\n");
    $("config").textContent = m.config.map((c) => `${c.key} = ${c.value}`).join("\n");
  }
  if (state && heavy && state.meshContig >= 40) {
    $("result").textContent = JSON.stringify({
      ok: state.meshContig >= 40 && state.fleet.length === 3 && state.config.length >= 2 && maxTaskMs < 100,
      vfs: state.vfs,
      writerId: state.writerId,
      resumedFromSeq: state.resumedFromSeq,
      meshContig: state.meshContig,
      fleetMachines: state.fleet.length,
      configKeys: state.config.length,
      heavyQueryMs: heavy.heavyMs,
      mainThread: { longTasks, maxTaskMs: Math.round(maxTaskMs) },
    });
  }
};
