import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

// Builds an unsigned .dbxp review candidate for the current (or DBX_PLUGIN_TARGET)
// platform: cross-compiles the Go sidecar, stages manifest + assets + ui, then
// runs the repository packager. Signing is a separate, maintainer-only step.

const root = path.dirname(fileURLToPath(import.meta.url));
const dist = process.env.DBX_PLUGIN_OUTPUT_DIR ? path.resolve(process.env.DBX_PLUGIN_OUTPUT_DIR) : path.join(root, "dist");
const buildRoot = await mkdtemp(path.join(os.tmpdir(), "dbx-my2sql-package-"));
const stage = path.join(buildRoot, "stage");
const detectedTarget = platformTarget();
const target = process.env.DBX_PLUGIN_TARGET || detectedTarget;
const exeName = target.startsWith("windows") ? "my2sql-sidecar.exe" : "my2sql-sidecar";
const output = path.join(dist, `io.github.my2sql-1.0.0-${target}.dbxp`);
const artifactMetadata = output.replace(/\.dbxp$/, ".artifact.json");

if (process.env.DBX_PLUGIN_SIGNING_KEY || process.env.DBX_PLUGIN_SIGNING_KEY_ID) {
  throw new Error("package.mjs only builds unsigned review candidates; signing happens in the store workflow.");
}

const [goos, goarch] = splitTarget(target);
const env = { ...process.env, CGO_ENABLED: "0", GOOS: goos, GOARCH: goarch, GOFLAGS: "-mod=mod" };

// The binlog kernel is gated behind the `my2sql` build tag so the sidecar can be
// packaged with only the standard library when the go-mysql module is not yet
// fetched. Set DBX_MY2SQL_TAGS=my2sql (in a networked build) to link the real
// parsing engine into the shipped binary.
const goTags = process.env.DBX_MY2SQL_TAGS || "";
const goBuildArgs = ["build", "-trimpath"];
if (goTags) goBuildArgs.push("-tags", goTags);
goBuildArgs.push("-ldflags", "-s -w", "-o", path.join(stage, "bin", target, exeName), ".");

try {
  await mkdir(path.join(stage, "bin", target), { recursive: true });
  await mkdir(dist, { recursive: true });

  run("go", goBuildArgs, env, path.join(root, "backend"));

  const manifest = JSON.parse(await readFile(path.join(root, "manifest.json"), "utf8"));
  manifest.entrypoints.backend = { executable: `bin/${target}/${exeName}`, transport: "stdio-jsonl" };
  await writeFile(path.join(stage, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  await cp(path.join(root, "assets"), path.join(stage, "assets"), { recursive: true });
  await cp(path.join(root, "ui"), path.join(stage, "ui"), { recursive: true });

  const packager = path.join(root, "..", "plugins", "sdk", "packager", "Cargo.toml");
  run("cargo", ["run", "--release", "--manifest-path", packager, "--", stage, output, "--artifact-metadata", artifactMetadata, "--target", target, "--artifact-url", path.basename(output)], env, root);
  console.log(`Built unsigned candidate ${output}`);
} finally {
  await rm(buildRoot, { recursive: true, force: true });
}

function run(command, args, extraEnv, cwd) {
  const result = spawnSync(command, args, { cwd, env: { ...process.env, ...extraEnv }, stdio: "inherit", shell: process.platform === "win32" });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

function splitTarget(t) {
  const [osName, arch] = t.split("-");
  const goos = { darwin: "darwin", linux: "linux", windows: "windows" }[osName];
  const goarch = { arm64: "arm64", x64: "amd64" }[arch];
  if (!goos || !goarch) throw new Error(`Unsupported plugin target: ${t}`);
  return [goos, goarch];
}

function platformTarget() {
  const osName = { darwin: "darwin", linux: "linux", win32: "windows" }[process.platform];
  const arch = { arm64: "arm64", x64: "x64" }[process.arch];
  if (!osName || !arch) throw new Error(`Unsupported plugin target: ${process.platform}-${process.arch}`);
  return `${osName}-${arch}`;
}
