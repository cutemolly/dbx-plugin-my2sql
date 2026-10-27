#!/usr/bin/env node
// Online-job lifecycle reproduction against a real MySQL (local 3307).
//
// Question it answers: when an online (repl) analysis job is running, does the
// sidecar process stay alive? The DBX Store review flow and the user's DBX run
// showed the workbench reporting "unknown job: <id>" after pressing 停止, which
// can only happen if the job table in the process that answers job/stop is empty
// — i.e. the process that ran the job is gone (DBX respawns with backoff, and the
// in-memory job map is lost).
//
// It drives the shipped protocol end to end:
//   connect -> precheck/list -> fixture (through exec/prepare + exec/start)
//   -> job/start (online, open-ended window) -> poll job/status while watching
//   for a process exit -> job/stop.
// Any stderr from the sidecar is echoed verbatim, so a Go panic surfaces here.
//
// Credentials come from repro.local.env (gitignored) and are never printed.
//
//   node repro-online-job.mjs
//   GO_BIN=/path/go node repro-online-job.mjs
import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(fileURLToPath(import.meta.url));
const goBin = process.env.GO_BIN || "go";
const envFile = path.join(root, "repro.local.env");
if (!existsSync(envFile)) {
  console.error(`missing ${envFile} — copy repro.local.env.example and fill it in`);
  process.exit(2);
}
const cfg = Object.fromEntries(
  readFileSync(envFile, "utf8").split("\n").filter((l) => l && !l.startsWith("#")).map((l) => {
    const i = l.indexOf("=");
    return [l.slice(0, i).trim(), l.slice(i + 1).trim()];
  }),
);
const host = cfg.MY2SQL_REPRO_HOST || "127.0.0.1";
const port = Number(cfg.MY2SQL_REPRO_PORT || 3307);
const user = cfg.MY2SQL_REPRO_USER;
const password = cfg.MY2SQL_REPRO_PASSWORD ?? "";
if (!user) { console.error("MY2SQL_REPRO_USER is empty"); process.exit(2); }
const scratchDb = "my2sql_repro";

const work = mkdtempSync(path.join(tmpdir(), "my2sql-repro-"));
const exe = path.join(work, "sidecar");

const build = spawnSync(goBin, ["build", "-tags", "my2sql", "-o", exe, "."], {
  cwd: path.join(root, "backend"),
  env: { ...process.env, CGO_ENABLED: "0", GOFLAGS: "-mod=mod" },
  stdio: "inherit",
});
if (build.status !== 0) { console.error("build failed"); process.exit(1); }

const child = spawn(exe, [], { env: { ...process.env, DBX_PLUGIN_DATA_DIR: work } });
let exitInfo = null;
child.on("exit", (code, signal) => {
  exitInfo = { code, signal };
  console.log(`\n!!! SIDECAR EXITED code=${code} signal=${signal}`);
});
child.stderr.on("data", (d) => process.stdout.write(`[sidecar] ${d}`));

let nextId = 1;
const pending = new Map();
const events = [];
let buf = "";
child.stdout.on("data", (chunk) => {
  buf += chunk.toString("utf8");
  let nl;
  while ((nl = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, nl).trim();
    buf = buf.slice(nl + 1);
    if (!line) continue;
    let msg;
    try { msg = JSON.parse(line); } catch { console.log(`!!! non-JSON stdout: ${line.slice(0, 160)}`); continue; }
    if (msg.id !== undefined && pending.has(msg.id)) { pending.get(msg.id)(msg); pending.delete(msg.id); }
    else if (msg.method) { events.push(msg); }
  }
});
function send(method, params, ms = 20000) {
  const frame = { jsonrpc: "2.0", method, params: params ?? {} };
  frame.id = nextId++;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout: ${method}`)), ms);
    pending.set(frame.id, (msg) => { clearTimeout(timer); resolve(msg); });
    child.stdin.write(JSON.stringify(frame) + "\n");
  });
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const unwrap = (msg, what) => {
  if (msg.error) { throw new Error(`${what}: ${msg.error.message}`); }
  return msg.result;
};
const stamp = () => new Date().toISOString().slice(11, 19);
const local = (d) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")} ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;

const connPayload = {
  connection: { id: "src", name: "repro", host, port, username: user, password, external_config: { server_id: 9901 } },
  runtime: { host, port },
};

async function runFixture() {
  // Seed binlog traffic through the plugin's own execution path: write a SQL file
  // into a valid job directory, then exec/prepare -> exec/start (confirm token).
  const jobId = "repro-fixture-1";
  const dir = path.join(work, "jobs", jobId);
  mkdirSync(dir, { recursive: true });
  writeFileSync(path.join(dir, "setup.sql"), [
    `CREATE DATABASE IF NOT EXISTS ${scratchDb};`,
    `CREATE TABLE IF NOT EXISTS ${scratchDb}.t1 (id INT PRIMARY KEY, name VARCHAR(64));`,
    `INSERT INTO ${scratchDb}.t1 (id, name) VALUES (1,'a'),(2,'b'),(3,'c');`,
    `DELETE FROM ${scratchDb}.t1 WHERE id = 2;`,
  ].join("\n"), { mode: 0o600 });

  const prep = unwrap(await send("exec/prepare", { jobId, file: "setup.sql", targetConnectionId: "src" }), "exec/prepare");
  const start = unwrap(await send("exec/start", { jobId, file: "setup.sql", targetConnectionId: "src", confirmToken: prep.confirmToken }), "exec/start");
  for (let i = 0; i < 20; i++) {
    const st = unwrap(await send("exec/status", { executionId: start.executionId }), "exec/status");
    if (st.status !== "running") {
      if (st.error) throw new Error(`fixture statements failed: ${st.error}`);
      console.log(`  fixture applied (${st.done}/${st.total})`);
      return;
    }
    await sleep(500);
  }
  throw new Error("fixture execution did not finish");
}

async function main() {
  console.log(`target ${host}:${port} user=${user} (password hidden)`);
  await send("plugin/initialize", { host: { protocolVersions: [1] } });
  unwrap(await send("connection/connect", connPayload), "connection/connect");
  console.log(`${stamp()} connected`);

  const pre = await send("binlog/precheck", { connectionId: "src" });
  console.log(`  precheck -> ${JSON.stringify(pre.result || pre.error)}`);
  const logs = await send("binlog/list", { connectionId: "src" });
  const names = (logs.result && (logs.result.files || logs.result.logs)) || [];
  console.log(`  binlog files -> ${names.length}${names.length ? ` (latest ${names[names.length - 1].name ?? names[names.length - 1]})` : ""}`);

  console.log(`${stamp()} seeding fixture through exec/prepare + exec/start`);
  await runFixture();

  // The user's scenario: online mode, 按时间 locator. Window starts in the past
  // and ends in the future so the job stays running while we watch the process.
  const from = new Date(Date.now() - 5 * 60 * 1000);
  const to = new Date(Date.now() + 5 * 60 * 1000);
  const opts = {
    sourceConnectionId: "src", mode: "repl", workType: "rollback",
    startDatetime: local(from), stopDatetime: local(to),
    databases: [scratchDb], tables: [], sqlTypes: ["insert", "update", "delete"],
  };
  console.log(`${stamp()} job/start online window ${opts.startDatetime} .. ${opts.stopDatetime}`);
  const started = unwrap(await send("job/start", opts), "job/start");
  const jobId = started.jobId;
  console.log(`  jobId ${jobId}`);

  let terminal = null;
  for (let i = 1; i <= 12; i++) {
    await sleep(2000);
    if (exitInfo) { console.log(`  t+${i * 2}s process is GONE (exit ${JSON.stringify(exitInfo)})`); break; }
    const st = await send("job/status", { jobId });
    const status = st.result ? st.result.status : `error: ${st.error && st.error.message}`;
    console.log(`  t+${i * 2}s alive status=${status}`);
    if (st.result && st.result.status !== "running") { terminal = st.result; break; }
  }
  if (terminal) {
    console.log(`  job finished early: ${JSON.stringify(terminal.result || terminal)}`);
  }

  console.log(`${stamp()} job/stop ${jobId}`);
  const stop = await send("job/stop", { jobId });
  console.log(`  job/stop -> ${JSON.stringify(stop.result || stop.error)}`);
  const again = await send("job/status", { jobId });
  console.log(`  job/status -> ${JSON.stringify(again.result || again.error)}`);

  const doneEvent = events.find((e) => e.method === "my2sql/job/done");
  console.log(`  job/done event seen: ${doneEvent ? JSON.stringify(doneEvent.params).slice(0, 300) : "NO"}`);
  console.log(`\nRESULT: process ${exitInfo ? `DIED during the job (${JSON.stringify(exitInfo)})` : "survived"}; job/stop ${stop.error ? "FAILED: " + stop.error.message : "ok"}`);
}

main()
  .catch((e) => console.error(`repro failed: ${e.message}`))
  .finally(async () => {
    child.stdin.end();
    child.kill();
    rmSync(work, { recursive: true, force: true });
    process.exit(0);
  });
