import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

// Builds an unsigned .dbxp review candidate for the current (or DBX_PLUGIN_TARGET)
// platform with the official DBX plugin CLI (@dbx-app/plugin-cli). The CLI
// cross-compiles the Go sidecar from dbx-plugin.toml, stages manifest + assets +
// ui, and runs the packager. Signing is a separate, maintainer-only step in the
// DBX Store workflow; this script refuses to run with signing inputs present.

const root = path.dirname(fileURLToPath(import.meta.url));
const dist = process.env.DBX_PLUGIN_OUTPUT_DIR ? path.resolve(process.env.DBX_PLUGIN_OUTPUT_DIR) : path.join(root, "dist");
const detectedTarget = platformTarget();
const target = process.env.DBX_PLUGIN_TARGET || detectedTarget;

if (process.env.DBX_PLUGIN_SIGNING_KEY || process.env.DBX_PLUGIN_SIGNING_KEY_ID) {
  throw new Error("package.mjs only builds unsigned review candidates; signing happens in the store workflow.");
}

const manifest = JSON.parse(readFileSync(path.join(root, "manifest.json"), "utf8"));
const output = path.join(dist, `${manifest.id}-${manifest.version}-${target}.dbxp`);

// The binlog kernel is gated behind the `my2sql` build tag. Set
// DBX_MY2SQL_TAGS=my2sql (in a networked build) to link the real go-mysql
// parsing engine into the shipped binary; the default build stays the
// dependency-free placeholder engine. The Go toolchain inherits GOFLAGS, and
// the CLI calls `go build` directly, so the tag flows through here.
const env = { ...process.env, CGO_ENABLED: "0" };
const goTags = process.env.DBX_MY2SQL_TAGS || "";
if (goTags) {
  env.GOFLAGS = env.GOFLAGS ? `${env.GOFLAGS} -tags=${goTags}` : `-tags=${goTags}`;
}

const cli = process.env.DBX_PLUGIN_CLI || "dbx-plugin";
// Probe without a shell so a missing CLI surfaces as ENOENT instead of a shell exit code.
const cliProbe = spawnSync(cli, ["--help"], { encoding: "utf8" }).error;
if (!cliProbe) {
  // The CLI packages from the project root; --target also accepts DBX_PLUGIN_TARGET.
  const result = spawnSync(cli, ["package", ".", "--target", target, "--output-dir", dist, "--artifact-url", path.basename(output)], { cwd: root, env, stdio: "inherit", shell: process.platform === "win32" });
  if (result.status !== 0) process.exit(result.status ?? 1);
} else {
  // Fallback for checkouts beside the dbx repository: drive the Rust packager with cargo.
  const packager = process.env.DBX_PLUGIN_PACKAGER_MANIFEST || path.join(root, "..", "plugins", "sdk", "packager", "Cargo.toml");
  if (!existsSync(packager)) {
    throw new Error(`Neither the dbx-plugin CLI nor the dbx repository packager (${packager}) was found. Install @dbx-app/plugin-cli or set DBX_PLUGIN_PACKAGER_MANIFEST.`);
  }
  const result = spawnSync("cargo", ["run", "--release", "--manifest-path", packager, "--", ".", "--target", target, "--output-dir", dist, "--artifact-url", path.basename(output)], { cwd: root, env, stdio: "inherit", shell: process.platform === "win32" });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

console.log(`Built unsigned candidate ${output}`);

function platformTarget() {
  const osName = { darwin: "darwin", linux: "linux", win32: "windows" }[process.platform];
  const arch = { arm64: "arm64", x64: "x64" }[process.arch];
  if (!osName || !arch) throw new Error(`Unsupported plugin target: ${process.platform}-${process.arch}`);
  return `${osName}-${arch}`;
}
