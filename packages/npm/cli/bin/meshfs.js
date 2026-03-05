#!/usr/bin/env node

const { spawnSync } = require("node:child_process");
const {
  resolveBinaryPath,
  formatResolutionError
} = require("../lib/resolve-binary");

let binaryPath;
try {
  binaryPath = resolveBinaryPath();
} catch (error) {
  process.stderr.write(formatResolutionError(error));
  process.exit(1);
}

const result = spawnSync(binaryPath, process.argv.slice(2), {
  stdio: "inherit"
});

if (result.error) {
  process.stderr.write(`failed to execute meshfs binary: ${result.error.message}\n`);
  process.exit(1);
}

if (typeof result.status === "number") {
  process.exit(result.status);
}

if (result.signal) {
  process.stderr.write(`meshfs process terminated by signal: ${result.signal}\n`);
}
process.exit(1);
