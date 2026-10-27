<!--
Plugin submission: add publishers/<publisher-id>.json (first submission only) and
candidates/<plugin-id>.json. Do not edit plugins/*.json or catalog/index.json —
the DBX Store signing workflow finalizes them after review.
-->

## Plugin submission

- Plugin ID: `io.github.my2sql`
- Version: `1.0.1`
- Publisher ID: `cutemolly`

This supersedes PR #364 (`1.0.0`), which was blocked by a security review. All
findings from that review are fixed here; see "Security review fixes" below.

## Source repository and tag

Source: https://github.com/cutemolly/dbx-plugin-my2sql
Tag the candidates belong to: `v1.0.1` (annotated; `git rev-parse v1.0.1^{commit}`
resolves to the same commit as `main`, so the tag, the branch and this submission
bundle are one state).

> Build provenance, stated exactly:
> - The five attached candidates were built by the official reusable workflow in
>   run https://github.com/cutemolly/dbx-plugin-my2sql/actions/runs/40454704037,
>   dispatched on ref `v1.0.1`, head commit `ae0e7c3`.
> - `v1.0.1` was first cut at `254e02c` for the initial submission, then moved
>   when review round 2 and local testing surfaced further defects (a whole-file
>   read that could abort the sidecar, lost output on manual stop, the deletion
>   path below). The version number stayed `1.0.1`: `1.0.1` was never listed or
>   signed, so this is a pre-publication correction, not a rewrite of a published
>   release.
> - The tag commit may differ from `ae0e7c3` only in the store submission files
>   (`store/candidate.json`, `store/submission/PR_BODY.md`,
>   `store/submission/candidates/`) — `git diff --name-only ae0e7c3 v1.0.1^{commit}`
>   shows nothing else. None of those files are packaged: the `.dbxp` contains
>   only `manifest.json`, `ui/index.html`, the two icons, `LICENSE`,
>   `THIRD_PARTY_NOTICES.md`, `checksums.json` and `bin/<target>/my2sql-sidecar`.
>   Every code, UI and build input is identical.
> - The packager is **not** byte-reproducible. Archive entries are timestamp-
>   normalized, but the compiled sidecar is not: two workflow runs whose build
>   inputs were identical produced the same size on three targets and 1–2 bytes
>   apart on two, with all five digests different. The digests pinned in
>   `candidates/io.github.my2sql.json` are therefore the digests of the bytes
>   actually published under Release `v1.0.1` — `gh release download v1.0.1` plus
>   `shasum -a 256` reproduces the comparison — and match the workflow-generated
>   `release-candidates.json` attached to the same release. A maintainer re-running
>   the sign step against these pinned bytes is safe; re-running the *build* is not
>   expected to reproduce them.

Unsigned candidate assets and the merged `release-candidates.json` are attached to
GitHub Release `v1.0.1`:
https://github.com/cutemolly/dbx-plugin-my2sql/releases/tag/v1.0.1

The release was produced by the official reusable workflow
(`t8y2/dbx/.github/workflows/plugin-release-reusable.yml@plugin-cli-v0.1.2`),
which builds one candidate per target on that target's own runner (linux-x64,
linux-arm64, windows-x64, darwin-x64, darwin-arm64 — each on its native image,
no cross-compilation).

## Capabilities and user workflow

MySQL binlog toolkit (viewer, SQL/rollback generator, and flashback executor).
Contributions declared in the manifest:

- **connection-provider** `io.github.my2sql.source` — a MySQL instance whose
  binlog can be read (replication user needs SELECT, REPLICATION SLAVE,
  REPLICATION CLIENT). Users fill host/port/user/password/server-id/timezone,
  plus a per-connection **TLS** mode (`off` / `require` / `verify`, new in 1.0.1).
- **workbench** `io.github.my2sql.main` — parse binlog **online** (as a fake
  replica via `COM_BINLOG_DUMP`) or **offline** (local binlog files); emit raw
  SQL, rollback SQL, or DML/big-transaction statistics; then optionally execute
  the rollback against a chosen target connection behind a confirmation gate.
- **context-menu** entries on a connection ("Analyze binlog") and on a table
  ("Roll back this table") that open the workbench.

## Permissions, data access, and network access

Manifest permissions (`permissions` in `candidates/io.github.my2sql.json`):

- `host.connections:read` — reads the user's DBX connection list so the plugin can
  resolve the source connection and pick a target connection for rollback
  execution. Credentials are supplied by the DBX host at connect time; the plugin
  never stores DBX connection passwords itself.
- `host.events` — receives host/UI events (job progress, selection changes).

(`host.storage` was declared in 1.0.0 but unused; it has been removed. The
plugin keeps its state in the host-provided plugin data directory.)

Data access:
- Reads binlog events from the user-configured MySQL source.
- Queries `INFORMATION_SCHEMA` on the source for column metadata (binlog
  `table_map` events do not carry column names).
- Writes generated `.sql` files and a `my2sql.stats.csv` under the plugin output
  directory, `jobs/<jobId>/`, with restrictive permissions (job dir `0700`,
  files `0600`). The UI offers a "copy path" action.
- Rollback execution is opt-in and gated server-side in the sidecar:
  `exec/prepare` mints a one-shot `confirmToken` bound to the exact
  (jobId, file, target) triple; `exec/start` only runs when presented with an
  unconsumed, unexpired (10 min) matching token.

Network access:
- Outbound TCP to the user-configured MySQL source `host:port` (replication dump +
  schema queries). When TLS is enabled (`require`/`verify`), both the metadata
  session and the `COM_BINLOG_DUMP` replication stream are wrapped in TLS
  (`go-sql-driver/mysql` registered TLS config + `go-mysql`
  `BinlogSyncerConfig.TLSConfig`).
- Outbound TCP to the user-selected target MySQL connection for rollback execution
  (same TLS option applies).
- SSH tunneling is provided by the DBX host itself: the plugin always dials the
  `runtime.host:runtime.port` endpoint that DBX resolves after its tunnel/proxy
  layers, so an SSH-tunneled connection needs no plugin-side support.
- No third-party APIs, telemetry, or background network activity.

## Native sidecar behavior

Yes — one native Go executable `bin/<target>/my2sql-sidecar` (stdio-jsonl
transport, launched by the DBX host).

- **Subprocesses:** none (the sidecar does not spawn external commands; binlog
  parsing uses the in-process `go-mysql` library behind the `my2sql` build tag).
- **Listening ports:** none (outbound MySQL client connections only; it never
  binds a socket).
- **Filesystem writes:** the plugin output directory only — generated SQL files
  and the stats CSV (job dir `0700`, files `0600`). `jobId` is validated against
  `[A-Za-z0-9_-]{1,64}` and the final path is checked to stay under `jobs/`
  before any file I/O. No writes outside the host provided output path.
- **External commands:** none.

## Security review fixes (1.0.1)

1. **jobId path traversal** — the path resolver (the single choke point for
   `job/output/list`, `job/output/preview`, `job/delete`, `exec/prepare`,
   `exec/start`) rejects any id outside `[A-Za-z0-9_-]{1,64}` and re-checks the
   joined path stays directly under `jobs/`. It is now split into a validation-only
   `jobOutputPath` (all read and delete paths) and `jobOutputDir` (validate +
   create, used only when an engine is about to write), so no read path can
   recreate a directory. Regression tests cover the original
   `../../../../../../etc` + `hosts` reproduction at both unit and RPC level
   (`backend/security_test.go`, `security-smoke.mjs`).
2. **UI HTML injection** — every server-controlled value rendered into
   `innerHTML` is escaped; the binary-log file names from `SHOW BINARY LOGS`
   (line 403) now go through `escapeHtml`.
3. **Server-side execution confirmation** — the confirm gate moved from the UI
   into the sidecar as a one-shot token state machine (see "Data access" above);
   a script that invokes `exec/start` directly cannot skip `exec/prepare`.
4. **Manifest attribution** — `source`/`homepage` now point at this repository
   (they incorrectly referenced `liuhr/my2sql` in 1.0.0).
5. **License disclosure** — upstream `liuhr/my2sql` is MIT (not MPL-1.0 as the
   1.0.0 PR stated); the README and this PR are corrected. The packaged
   `.dbxp` now includes `LICENSE` and `THIRD_PARTY_NOTICES.md` with the full
   license text of all 19 statically linked Go modules (audited from the
   vendored module tree: go-sql-driver/mysql MPL-2.0; go-mysql MIT;
   klauspost/compress, pingcap/log, tidb/pkg/parser Apache-2.0; pingcap/errors
   BSD-2-Clause; google/uuid, golang.org/x/text, golang.org/x/exp,
   crypto/edwards25519 BSD-3-Clause; remainder MIT including Masterminds/semver).
6. **`host.storage`** — removed as unused.

## Review round 2 (folded into the `1.0.1` tag)

Found while testing the plugin against real traffic; none of these required a
version bump, so they were folded into `1.0.1` (see the build-provenance note
above).

1. **Whole-file reads could abort the sidecar.** `job/output/preview`,
   `exec/prepare` and `exec/start` read the entire output file into memory. A real
   analysis routinely produces multi-gigabyte rollback scripts (observed: 1.34 GB),
   which triggered a Go runtime allocation failure — not catchable by `recover()` —
   and recycled the process; the workbench then reported the harmless-looking
   `unknown job`. All three now stream the file through one shared statement/line
   scanner (`stmtSplitter`, `nextLine`, `scanLineWindow`), bounded by a 64 KB
   per-line display cap and a 64 MB per-statement hard stop for execution, so a
   statement is never silently truncated. Measured on the same 1.5 GB file: old
   path 4.0 GB live heap; new path 13 previews in 4 ms, process alive.
   Equivalence between the streaming and previous whole-string splitting is
   proven by a dedicated test (`backend/stream_test.go`).
2. **Manual stop kept no output.** The engine writes SQL while it parses, so a
   stopped job has real files; the cancellation error path discarded them. Stop now
   preserves the flushed files and statistics, the force-stop watchdog lists what
   is on disk, `isStopped` sees through wrapped context errors, and the UI renders
   output even when the result carries an error.
3. **History view.** Completed analyses are listed from the plugin data directory
   (they survive a sidecar restart), each entry carrying the connection identity it
   ran against — id, name, host, port only, never credentials — and a new
   `job/delete` RPC plus a two-step in-UI delete reclaims the disk.
4. **Panic isolation and disclosure.** `Handle`, the analysis engine and the
   execution goroutine each `recover()` (the Go SDK has no boundary of its own),
   so a panic fails one request or one job instead of the whole process. The panic
   value is written to the plugin log only: the RPC error, `job.json` and the UI
   get a generic message, because a kernel panic can carry SQL or connection text.
5. **UI injection surface.** Server-supplied statistics numbers, timestamps and
   file kinds interpolated into `innerHTML` are now escaped or coerced; the output
   preview became lazy (no automatic read of the first file on job completion).

## Known limitations

- Output files of a deleted analysis are removed with the history entry
  (`job/delete`), but a job left untouched persists in the plugin data directory
  after uninstall; there is no bulk-cleanup action yet.
- `verify` TLS mode validates the CA and server name against the system trust
  store; custom CA pinning via a file picker is not yet offered.
- `threads` is accepted and defaulted to 8 but the in-process parser currently
  runs single-stream; the value has no effect yet.

## License

MIT (see `LICENSE` in the source repository, also packaged inside the `.dbxp`).
Upstream inspiration [liuhr/my2sql](https://github.com/liuhr/my2sql) (MIT) is a
separate Go implementation; no source is copied — this codebase is an
independent reimplementation of the workflow (module
`github.com/db-plugins/my2sql`; reviewer-verified 6-gram intersection with the
upstream `base/*.go`/`main.go` is 0).

## Homepage or support URL

https://github.com/cutemolly/dbx-plugin-my2sql (issues tracked here)

## Confirmation

- [x] The candidate packages are unsigned and match the disclosed source (see the
      build-provenance block above: run `40454704037` on ref `v1.0.1`).
- [x] I have not included private keys, tokens, credentials, or proprietary data.
- [x] The candidates are listed in `candidates/io.github.my2sql.json`; `plugins/` and `catalog/` were not edited manually.
