import fs from "node:fs/promises";
import path from "node:path";

const packageDirArg = process.argv[2];
const sourceBinaryArg = process.argv[3];

if (!packageDirArg || !sourceBinaryArg) {
  throw new Error(
    "usage: node stage-platform-binary.mjs <platform-package-dir> <source-binary-path>"
  );
}

const packageDir = path.resolve(process.cwd(), packageDirArg);
const sourceBinary = path.resolve(process.cwd(), sourceBinaryArg);
const packageJsonPath = path.join(packageDir, "package.json");
const packageJsonRaw = await fs.readFile(packageJsonPath, "utf8");
const pkg = JSON.parse(packageJsonRaw);

const isWindows = Array.isArray(pkg.os) && pkg.os.includes("win32");
const binaryName = isWindows ? "meshfs.exe" : "meshfs";
const targetDir = path.join(packageDir, "bin");
const targetBinary = path.join(targetDir, binaryName);

await fs.mkdir(targetDir, { recursive: true });
await fs.copyFile(sourceBinary, targetBinary);

if (!isWindows) {
  await fs.chmod(targetBinary, 0o755);
}

process.stdout.write(
  `staged ${sourceBinary} -> ${targetBinary} for package ${pkg.name}\n`
);
