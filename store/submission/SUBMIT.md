# Submitting `io.github.my2sql` to the DBX Store

Everything here is already validated against the official
`t8y2/dbx-store` `scripts/validate.mjs` (see "Pre-validated" below). This
directory mirrors the exact store repository layout so the files can be copied
straight into a fork.

## Files (store layout)

| This path | Goes to in `t8y2/dbx-store` |
| --- | --- |
| `submission/candidates/io.github.my2sql.json` | `candidates/io.github.my2sql.json` |
| `submission/publishers/cutemolly.json` | `publishers/cutemolly.json` (first submission registers the publisher) |
| `submission/PR_BODY.md` | the pull request description |

`store/candidate.json` (one level up) is the same content kept in the plugin repo
for reference; the `submission/` copy is the one to paste into the fork.

Do **not** touch `plugins/*.json` or `catalog/index.json` — the DBX Store signing
workflow generates them after review.

## Steps

1. Fork the store and clone the fork (the `--remote` flag is unsupported when a
   repository argument is provided, so do not pass it):

   ```bash
   gh repo fork t8y2/dbx-store --clone
   ```

2. On a new branch in the fork, add the two files exactly at the store paths:

   ```
   candidates/io.github.my2sql.json
   publishers/cutemolly.json
   ```

   (copy from `submission/candidates/` and `submission/publishers/`.)

3. Open a PR against `t8y2/dbx-store:main` using `submission/PR_BODY.md` as the
   body.

4. CI runs `scripts/validate.mjs`. It will intentionally stay **red** with:
   `Catalog contains 1 open candidate(s) awaiting DBX Store signing:
   io.github.my2sql@1.0.1`
   That is the success signal — it means the candidate is structurally valid and
   is now waiting on a maintainer.

5. A DBX maintainer reviews the source tag and runs the protected
   **Sign plugin PR candidates** workflow (via a `/sign` PR comment). The workflow
   verifies the pinned SHA-256 bytes, signs them, publishes the signed assets,
   generates `plugins/io.github.my2sql.json` + `catalog/index.json`, removes
   `candidates/io.github.my2sql.json`, and pushes it all back to the PR.

6. Once CI turns green, the maintainer merges. The version becomes visible in the
   Marketplace.

## Notes

- There is no separate submission Issue and no signing secret on the author side.
- If the listing lacks a locale, maintainers may comment `/localize` to auto-fill
  translations.
- A published version is immutable: to change anything, bump `manifest.json`
  version, cut a new GitHub Release `vX.Y.Z` (the release workflow rebuilds the
  candidates), and submit a new `candidates/io.github.my2sql.json` for that version.

## Pre-validated

Locally run against a fresh clone of the store:

```
node scripts/validate.mjs   ->  only blocker = "awaiting DBX Store signing", 0 structural errors on our files
```
