import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const rawVersion = process.argv[2];
if (!rawVersion) {
  throw new Error("usage: node set-npm-version.mjs <version|vX.Y.Z>");
}

const version = rawVersion.startsWith("v") ? rawVersion.slice(1) : rawVersion;
const semverPattern =
  /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;
if (!semverPattern.test(version)) {
  throw new Error(`invalid version: ${rawVersion}`);
}

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const npmRoot = path.resolve(scriptDir, "..");

const packageJsonPaths = [
  path.join(npmRoot, "cli", "package.json"),
  path.join(npmRoot, "platforms", "darwin-x64", "package.json"),
  path.join(npmRoot, "platforms", "linux-x64", "package.json"),
  path.join(npmRoot, "platforms", "win32-x64", "package.json")
];

for (const packageJsonPath of packageJsonPaths) {
  const content = await fs.readFile(packageJsonPath, "utf8");
  const pkg = JSON.parse(content);
  pkg.version = version;

  if (pkg.name === "@meshfs/cli" && pkg.optionalDependencies) {
    for (const [dependencyName] of Object.entries(pkg.optionalDependencies)) {
      if (dependencyName.startsWith("@meshfs/cli-")) {
        pkg.optionalDependencies[dependencyName] = version;
      }
    }
  }

  await fs.writeFile(packageJsonPath, `${JSON.stringify(pkg, null, 2)}\n`, "utf8");
}

process.stdout.write(`synced npm package versions to ${version}\n`);
