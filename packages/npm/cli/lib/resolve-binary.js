const { existsSync } = require("node:fs");

const PLATFORM_PACKAGES = Object.freeze({
  "darwin-x64": "@meshfs/cli-darwin-x64",
  "linux-x64": "@meshfs/cli-linux-x64",
  "win32-x64": "@meshfs/cli-win32-x64"
});

function platformKey() {
  return `${process.platform}-${process.arch}`;
}

function resolveBinaryPackageName() {
  return PLATFORM_PACKAGES[platformKey()];
}

function resolveBinaryPath() {
  const packageName = resolveBinaryPackageName();
  if (!packageName) {
    throw new Error(
      `unsupported platform: ${process.platform}/${process.arch}; supported targets: ${Object.keys(PLATFORM_PACKAGES).join(", ")}`
    );
  }

  let binaryPath;
  try {
    binaryPath = require(packageName);
  } catch (error) {
    if (error && error.code === "MODULE_NOT_FOUND") {
      throw new Error(
        `platform package is not installed: ${packageName}; reinstall @meshfs/cli on a supported platform`
      );
    }
    throw error;
  }

  if (typeof binaryPath !== "string" || binaryPath.length === 0) {
    throw new Error(`invalid binary path export from package ${packageName}`);
  }

  if (!existsSync(binaryPath)) {
    throw new Error(`resolved meshfs binary does not exist: ${binaryPath}`);
  }

  return binaryPath;
}

function formatResolutionError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return `unable to launch meshfs: ${message}\n`;
}

module.exports = {
  resolveBinaryPackageName,
  resolveBinaryPath,
  formatResolutionError
};
