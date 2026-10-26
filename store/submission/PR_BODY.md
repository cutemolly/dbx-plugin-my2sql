<!--
Plugin submission: add publishers/<publisher-id>.json (first submission only) and
candidates/<plugin-id>.json. Do not edit plugins/*.json or catalog/index.json —
the DBX Store signing workflow finalizes them after review.
-->

## Plugin submission

- Plugin ID: `io.github.my2sql`
- Version: `1.0.0`
- Publisher ID: `cutemolly`

## Source repository and tag

Source: https://github.com/cutemolly/dbx-plugin-my2sql
Immutable tag the candidates were built from: `v1.0.0` (commit `7ef5d369ea4032eda564fe5376948014ff4dbb19`).

Unsigned candidate assets and the merged `release-candidates.json` are attached to
GitHub Release `v1.0.0`:
https://github.com/cutemolly/dbx-plugin-my2sql/releases/tag/v1.0.0

The release was produced by the official reusable workflow
(`t8y2/dbx/.github/workflows/plugin-release-reusable.yml@plugin-cli-v0.1.2`),
which builds one candidate per target on that target's own runner.

## Capabilities and user workflow

MySQL binlog toolkit (viewer, SQL/rollback generator, and flashback executor).
Contributions declared in the manifest:

- **connection-provider** `io.github.my2sql.source` — a MySQL instance whose
  binlog can be read (replication user needs SELECT, REPLICATION SLAVE,
  REPLICATION CLIENT). Users fill host/port/user/password/server-id/timezone.
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
- `host.storage` — plugin-private key-value storage for its own UI state/jobs.

Data access:
- Reads binlog events from the user-configured MySQL source.
- Queries `INFORMATION_SCHEMA` on the source for column metadata (binlog
  `table_map` events do not carry column names).
- Writes generated `.sql` files and a `my2sql.stats.csv` under the plugin output
  directory (user-visible; the UI offers a "copy path" action).
- Rollback execution is opt-in and gated: the plugin only runs the user-selected
  rollback SQL against the target connection the user explicitly chooses.

Network access:
- Outbound TCP to the user-configured MySQL source `host:port` (replication dump +
  schema queries).
- Outbound TCP to the user-selected target MySQL connection for rollback execution.
- No third-party APIs, telemetry, or background network activity.

## Native sidecar behavior

Yes — one native Go executable `bin/<target>/my2sql-sidecar` (stdio-jsonl
transport, launched by the DBX host).

- **Subprocesses:** none (the sidecar does not spawn external commands; binlog
  parsing uses the in-process `go-mysql` library behind the `my2sql` build tag).
- **Listening ports:** none (outbound MySQL client connections only; it never
  binds a socket).
- **Filesystem writes:** the plugin output directory — generated SQL files and the
  stats CSV (`os.MkdirAll` `0755`, job dir `0700`). No writes outside the host
  provided output path.
- **External commands:** none.

## License

MIT (see `LICENSE` in the source repository). Upstream inspiration
[liuhr/my2sql](https://github.com/liuhr/my2sql) (MPL-1.0) is a separate Go
reimplementation; no source is copied — this codebase is an independent
reimplementation of the workflow.

## Homepage or support URL

https://github.com/cutemolly/dbx-plugin-my2sql (issues tracked here)

## Confirmation

- [x] The candidate packages are unsigned and match the disclosed source tag.
- [x] I have not included private keys, tokens, credentials, or proprietary data.
- [x] The candidates are listed in `candidates/io.github.my2sql.json`; `plugins/` and `catalog/` were not edited manually.
