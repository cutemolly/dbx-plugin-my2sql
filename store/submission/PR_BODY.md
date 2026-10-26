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
Immutable tag the candidates were built from: `v1.0.1` (commit `254e02c63aca4ba570847fdf6c59616c3fde23d4`).
The tag was created **before** the release build and has not been moved.

Unsigned candidate assets and the merged `release-candidates.json` are attached to
GitHub Release `v1.0.1`:
https://github.com/cutemolly/dbx-plugin-my2sql/releases/tag/v1.0.1

The release was produced by the official reusable workflow
(`t8y2/dbx/.github/workflows/plugin-release-reusable.yml@plugin-cli-v0.1.2`),
which builds one candidate per target on that target's own runner.

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

1. **jobId path traversal** — `jobOutputDir` (the single choke point for
   `job/output/list`, `job/output/preview`, `exec/prepare`, `exec/start`) now
   rejects any id outside `[A-Za-z0-9_-]{1,64}` and re-checks the joined path
   stays directly under `jobs/`. Regression tests cover the original
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

## Known limitations

- Generated output files persist under the plugin data directory after
  uninstall; an in-UI cleanup entry is planned but not yet implemented.
- `verify` TLS mode validates the CA and server name against the system trust
  store; custom CA pinning via a file picker is not yet offered.

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

- [x] The candidate packages are unsigned and match the disclosed source tag.
- [x] I have not included private keys, tokens, credentials, or proprietary data.
- [x] The candidates are listed in `candidates/io.github.my2sql.json`; `plugins/` and `catalog/` were not edited manually.
