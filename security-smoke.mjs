#!/usr/bin/env node
// RPC-level security smoke for the my2sql sidecar.
//
// It drives the real stdio JSON-RPC dispatch (main.go handlers) to prove the
// three review findings are fixed at the wire boundary, not just in unit tests:
//
//   1. jobId path traversal — job/output/list, job/output/preview, exec/prepare
//      and exec/start all resolve their directory through jobOutputDir, which now
//      rejects ids outside [A-Za-z0-9_-]{1,64}. The reviewer climbed out with
//      "../../../../../../etc" + file "hosts"; that must now error.
//   2. exec confirmation moved into the sidecar — exec/start requires a one-shot
//      confirmToken minted by exec/prepare and bound to the exact triple.
//   3. HTML injection is a UI concern (see ui/index.html escapeHtml on f.name);
//      not exercised here.
//
//   node security-smoke.mjs
//   GO_BIN=/path/go node security-smoke.mjs
import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const backend = path.join(root, "backend");
const goBin = process.env.GO_BIN || "go";
const work = mkdtempSync(path.join(tmpdir(), "my2sql-sec-"));
const exe = path.join(work, process.platform === "win32" ? "my2sql-sidecar.exe" : "my2sql-sidecar");

let failures = 0;
function check(name, ok, detail) {
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}${detail ? `  (${detail})` : ""}`);
  if (!ok) failures++;
}

const build = spawnSync(goBin, ["build", "-o", exe, "."], {
  cwd: backend,
  env: { ...process.env, CGO_ENABLED: "0", GOFLAGS: "-mod=mod", GOPROXY: process.env.GOPROXY || "off", GOSUMDB: "off" },
  stdio: "inherit",
});
if (build.status !== 0) { console.error("build failed"); rmSync(work, { recursive: true, force: true }); process.exit(1); }

const child = spawn(exe, [], { env: { ...process.env, DBX_PLUGIN_DATA_DIR: work } });
let nextId = 1;
const pending = new Map();
let buf = "";
child.stdout.on("data", (chunk) => {
  buf += chunk.toString("utf8");
  let nl;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1);
    if (!line) continue;
    let msg; try { msg = JSON.parse(line); } catch { continue; }
    if (msg.id !== undefined && pending.has(msg.id)) { pending.get(msg.id)(msg); pending.delete(msg.id); }
  }
});
child.stderr.on("data", (d) => process.env.VERBOSE && process.stderr.write(d));

function send(method, params) {
  const frame = { jsonrpc: "2.0", method, params: params ?? {} };
  frame.id = nextId++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout: ${method}`)), 8000);
    pending.set(frame.id, (msg) => { clearTimeout(timer); resolve(msg); });
    child.stdin.write(JSON.stringify(frame) + "\n");
  });
}

// An error whose code proves the sidecar rejected the request (not a crash and
// not a successful result carrying attacker data).
function rejected(msg) { return !!msg.error; }

async function main() {
  await send("plugin/initialize", { host: { protocolVersions: [1] } });

  // Open a saved connection so session-scoped methods get past the -32010 gate.
  // The endpoint is closed (port 1); none of these checks reach the network.
  await send("connection/connect", {
    connection: { id: "c1", name: "sec", host: "127.0.0.1", port: 1, username: "u", password: "p", external_config: { server_id: 9999 } },
    runtime: { host: "127.0.0.1", port: 1 },
  });

  // Seed a legitimate job dir + rollback file so the token flow has something real.
  const goodId = "20261026T000000-1";
  mkdirSync(path.join(work, "jobs", goodId), { recursive: true });
  writeFileSync(path.join(work, "jobs", goodId, "my2sql_rollback.sql"), "DELETE FROM t WHERE id=1;\n");

  const evil = "../../../../../../etc";

  // 1. jobId traversal on all four filesystem-facing RPCs.
  check("job/output/preview rejects traversal jobId", rejected(await send("job/output/preview", { jobId: evil, file: "hosts" })));
  check("job/output/list rejects traversal jobId", rejected(await send("job/output/list", { jobId: evil })));
  check("exec/prepare rejects traversal jobId", rejected(await send("exec/prepare", { jobId: evil, file: "hosts", targetConnectionId: "c1" })));
  check("exec/start rejects traversal jobId", rejected(await send("exec/start", { jobId: evil, file: "hosts", targetConnectionId: "c1", confirmToken: "x" })));

  // 2. exec confirmation moved into the sidecar.
  const noTok = await send("exec/start", { jobId: goodId, file: "my2sql_rollback.sql", targetConnectionId: "c1" });
  check("exec/start without token is refused", rejected(noTok), noTok.error && noTok.error.message);
  const badTok = await send("exec/start", { jobId: goodId, file: "my2sql_rollback.sql", targetConnectionId: "c1", confirmToken: "beef" });
  check("exec/start with unknown token is refused", rejected(badTok), badTok.error && badTok.error.message);
  const prep = await send("exec/prepare", { jobId: goodId, file: "my2sql_rollback.sql", targetConnectionId: "c1" });
  const token = prep.result && prep.result.confirmToken;
  check("exec/prepare mints a confirmToken", !!token, JSON.stringify(prep.result && prep.result.statementCount));
  const okStart = await send("exec/start", { jobId: goodId, file: "my2sql_rollback.sql", targetConnectionId: "c1", confirmToken: token });
  check("exec/start accepts the fresh token", !!okStart.result && !!okStart.result.executionId, JSON.stringify(okStart.result || okStart.error));
  const reuse = await send("exec/start", { jobId: goodId, file: "my2sql_rollback.sql", targetConnectionId: "c1", confirmToken: token });
  check("exec/start consumes the token (one-shot)", rejected(reuse), reuse.error && reuse.error.message);
}

main()
  .catch((e) => check("security smoke threw", false, e.message))
  .finally(() => {
    child.stdin.end(); child.kill(); rmSync(work, { recursive: true, force: true });
    console.log(`\n${failures === 0 ? "ALL PASS" : failures + " FAILURE(S)"}`);
    process.exit(failures === 0 ? 0 : 1);
  });
