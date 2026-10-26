# dbx-plugin-my2sql

my2sql Binlog Toolkit — a [DBX](https://github.com/t8y2/dbx) plugin for MySQL
binlog inspection and flashback. It parses binlog either **online** (as a fake
replica via `COM_BINLOG_DUMP`) or **offline** (local binlog files), and generates:

- original SQL (`--sql-type RAW`)
- rollback SQL (`--sql-type ROLLBACK`), optionally executed against a target connection behind an approval gate
- DML / big-transaction statistics

The toolkit is inspired by [liuhr/my2sql](https://github.com/liuhr/my2sql)
(MySQL binlog to SQL flashback tool); this plugin reimplements the workflow in
Go with a DBX workbench UI. Binlog parsing uses
[go-mysql](https://github.com/go-mysql-org/go-mysql) behind the `my2sql` build
tag, so the default tree builds with only the standard library plus the DBX Go
plugin SDK (vendored under `backend/dbx-plugin-sdk`).

## Layout

| Path | Purpose |
| --- | --- |
| `manifest.json` | DBX plugin manifest (ID `io.github.my2sql`) |
| `dbx-plugin.toml` | `dbx-plugin` CLI project config (Go backend: `backend/` → `my2sql-sidecar`) |
| `backend/` | Go sidecar: JSONL stdio protocol, binlog engines, SQL generator, job runner |
| `ui/index.html` | Sandbox workbench UI |
| `package.mjs` | Builds one unsigned `.dbxp` review candidate via the `dbx-plugin` CLI |
| `store/candidate.example.json` | Template for the DBX Store candidate metadata |

## Development

Requirements: Go ≥ 1.22, Node ≥ 22, and the DBX plugin CLI
(`npm install --global @dbx-app/plugin-cli`).

```bash
cd backend
go build ./...                 # default build (placeholder engine)
go test ./...                  # unit tests + protocol smoke test
GOFLAGS=-tags=my2sql go build ./...   # link the real go-mysql binlog kernel

node smoke.mjs                 # sidecar JSONL protocol smoke test
node package.mjs               # build an unsigned .dbxp candidate for this host
```

To exercise the plugin inside a running DBX desktop host, use
`dbx-plugin dev .` (add `GOFLAGS="-tags=my2sql"` for the real kernel).

## Release & store submission

`.github/workflows/release.yml` delegates to the official reusable workflow
(`t8y2/dbx/.github/workflows/plugin-release-reusable.yml`), which builds one
unsigned candidate per target (`darwin-arm64`, `darwin-x64`, `windows-x64`,
`linux-x64`, `linux-arm64`), merges `release-candidates.json`, and attaches
everything to the GitHub Release. Authors never sign; signing is done by DBX
Store maintainers after review.

Publishing a version:

1. Bump `version` in `manifest.json` (and update `releaseNotes`).
2. Tag and create a GitHub Release named `v<version>`.
3. The release workflow attaches the `.dbxp` candidates and `release-candidates.json`.
4. Fork [`t8y2/dbx-store`](https://github.com/t8y2/dbx-store) and open a PR adding
   `candidates/io.github.my2sql.json` (see `store/candidate.example.json`; on
   first submission also add `publishers/<publisher-id>.json`). CI stays red
   with "awaiting DBX Store signing" until a maintainer runs the protected
   signing workflow — that is expected.

Rebuilding a released version in place is not allowed; publish a new semantic
version instead.

## License

MIT (see [LICENSE](LICENSE)). This codebase is an independent Go
reimplementation of the my2sql binlog-to-SQL workflow and does not copy the
source of the [liuhr/my2sql](https://github.com/liuhr/my2sql) tool that inspired
it; that upstream project is likewise MIT-licensed. Third-party Go dependencies
and their licenses are listed in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
