#!/usr/bin/env node
// Protocol smoke test for the my2sql sidecar.
//
// It builds the default (no-tag) sidecar and drives the stdio JSON-RPC contract
// end-to-end without needing a real MySQL: it asserts the handshake, method
// dispatch, the two error codes the plugin relies on (-32601 unknown method,
// -32010 connection-not-open), the connection lifecycle event, and that a query
// against a closed endpoint surfaces as -32000. This is the fast, dependency-free
// counterpart to the live `-tags my2sql` integration test.
//
//   node smoke.mjs            # builds then runs all assertions
//   GO_BIN=/path/go node smoke.mjs
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const backend = path.join(root, "backend");
const goBin = process.env.GO_BIN || "go";
const work = mkdtempSync(path.join(tmpdir(), "my2sql-smoke-"));
const exe = path.join(work, process.platform === "win32" ? "my2sql-sidecar.exe" : "my2sql-sidecar");

let failures = 0;
function check(name, ok, detail) {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? `  (${detail})` : ""}`);
  if (!ok) failures++;
}

// 1. Build the default sidecar.
const build = spawnSync(goBin, ["build", "-o", exe, "."], {
  cwd: backend,
  env: { ...process.env, CGO_ENABLED: "0", GOFLAGS: "-mod=mod", GOPROXY: process.env.GOPROXY || "off", GOSUMDB: "off" },
  stdio: "inherit",
});
if (build.status !== 0) {
  console.error("build failed");
  rmSync(work, { recursive: true, force: true });
  process.exit(1);
}

// 2. Spawn and speak the protocol.
const child = spawn(exe, [], { env: { ...process.env, DBX_PLUGIN_DATA_DIR: work } });
let nextId = 1;
const pending = new Map(); // id -> resolve
const notifications = []; // events without an id
let buf = "";

child.stdout.on("data", (chunk) => {
  buf += chunk.toString("utf8");
  let nl;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).trim();
    buf = buf.slice(nl + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { continue; }
    if (msg.id !== undefined && pending.has(msg.id)) {
      pending.get(msg.id)(msg);
      pending.delete(msg.id);
    } else if (msg.method) {
      notifications.push(msg);
    }
  }
});
child.stderr.on("data", (d) => process.env.MY2SQL_SMOKE_VERBOSE && process.stderr.write(d));

function send(method, params, expectId = true) {
  const frame = { jsonrpc: "2.0", method, params: params ?? {} };
  if (expectId) {
    frame.id = nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`timeout waiting for ${method}`)), 8000);
      pending.set(frame.id, (msg) => { clearTimeout(timer); resolve(msg); });
      child.stdin.write(JSON.stringify(frame) + "\n");
    });
  }
  child.stdin.write(JSON.stringify(frame) + "\n");
}

const waitEvent = (method, ms = 2000) =>
  new Promise((resolve) => {
    const start = Date.now();
    const tick = setInterval(() => {
      const hit = notifications.find((n) => n.method === method);
      if (hit) { clearInterval(tick); resolve(hit); }
      else if (Date.now() - start > ms) { clearInterval(tick); resolve(null); }
    }, 25);
  });

async function main() {
  // Handshake: the SDK names this method plugin/initialize and echoes version + id.
  const init = await send("plugin/initialize", { host: { protocolVersions: [1] } });
  check("initialize responds", init.result && init.result.protocolVersion === 1, JSON.stringify(init.result && init.result.plugin));

  // Unknown method maps to JSON-RPC method-not-found.
  const unknown = await send("nope/missing", {});
  check("unknown method -> -32601", unknown.error && unknown.error.code === -32601, unknown.error && unknown.error.message);

  // A call that needs an open connection returns -32010 before any I/O.
  const noConn = await send("binlog/list", { connectionId: "ghost" });
  check("closed connection -> -32010", noConn.error && noConn.error.code === -32010, noConn.error && noConn.error.message);

  // Connection lifecycle stores the spec and emits a change event.
  const connPayload = {
    connection: { id: "c1", name: "smoke", host: "127.0.0.1", port: 1, username: "u", password: "p", external_config: { server_id: 9999 } },
    runtime: { host: "127.0.0.1", port: 1 },
  };
  const conn = await send("connection/connect", connPayload);
  check("connect succeeds", conn.result && conn.result.success === true, JSON.stringify(conn.result));
  const ev = await waitEvent("my2sql/connectionChanged");
  check("connect emits event", !!ev, ev && JSON.stringify(ev.params));

  // With the (closed) connection registered, a real query fails at the driver -> -32000.
  const meta = await send("meta/databases", { connectionId: "c1" });
  check("query on closed endpoint -> -32000", meta.error && meta.error.code === -32000, meta.error && meta.error.message);

  // Disconnect cleans up and notifies.
  const disc = await send("connection/disconnect", { connection: { id: "c1" } });
  check("disconnect succeeds", disc.result && disc.result.success === true, JSON.stringify(disc.result));

  // The placeholder engine reports an actionable M2 wiring message, not a crash.
  const job = await send("job/start", { sourceConnectionId: "c1", mode: "repl", workType: "rollback", databases: ["x"] });
  check("job on closed connection errors cleanly", !!job.error, job.error && job.error.message);

  // The history view answers over the request channel with nothing running: that
  // is what lets the workbench rebuild its job list after a sidecar restart.
  const hist = await send("job/history", { limit: 5 });
  check("job/history returns a jobs array", !!hist.result && Array.isArray(hist.result.jobs), JSON.stringify(hist.result));

  // Deleting an entry is the only way to reclaim the disk a wide analysis leaves
  // behind, and no read path may resurrect the directory it removed.
  const ghost = "20990101T000000-9";
  mkdirSync(path.join(work, "jobs", ghost), { recursive: true });
  writeFileSync(path.join(work, "jobs", ghost, "my2sql.forward.sql"), "-- seeded\n", { mode: 0o600 });
  const seeded = await send("job/history", { limit: 50 });
  check("job/history lists a directory with no live job", !!seeded.result && seeded.result.jobs.some((r) => r.jobId === ghost), JSON.stringify(seeded.result));
  const badDel = await send("job/delete", { jobId: "../outside" });
  check("job/delete rejects a traversal jobId", !!badDel.error && badDel.error.code === -32000, badDel.error && badDel.error.message);
  const del = await send("job/delete", { jobId: ghost });
  check("job/delete removes the job", !!del.result && del.result.success === true, JSON.stringify(del.result || del.error));
  const afterList = await send("job/output/list", { jobId: ghost });
  check("listing a deleted job fails instead of recreating it", !!afterList.error, JSON.stringify(afterList.result || afterList.error));
  const afterHist = await send("job/history", { limit: 50 });
  check("deleted job disappears from history", !!afterHist.result && !afterHist.result.jobs.some((r) => r.jobId === ghost), JSON.stringify(afterHist.result));
}

main()
  .catch((e) => { check("smoke run threw", false, e.message); })
  .finally(() => {
    child.stdin.end();
    child.kill();
    rmSync(work, { recursive: true, force: true });
    console.log(`\n${failures === 0 ? "ALL PASS" : failures + " FAILURE(S)"}`);
    process.exit(failures === 0 ? 0 : 1);
  });
