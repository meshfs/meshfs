# MeshFS npm Distribution

This directory contains the npm distribution layer for MeshFS CLI.

## Package Layout

- `cli`: top-level package (`@meshfs/cli`) that exposes the `meshfs` command.
- `platforms/linux-x64`: Linux x64 binary package.
- `platforms/darwin-x64`: macOS x64 binary package.
- `platforms/win32-x64`: Windows x64 binary package.
- `scripts`: release helper scripts for version sync and binary staging.

## Release Model

- Rust builds the native `meshfs` executable for each target.
- Each platform package includes exactly one native executable in `bin/`.
- `@meshfs/cli` depends on platform packages through `optionalDependencies`.
- The runtime launcher resolves and executes the installed platform binary.

## Publish Workflow

GitHub Actions workflow: `.github/workflows/npm-release.yml`

Requirements:

- npm Trusted Publishing configured for all `@meshfs/*` packages.
- GitHub tag release in format `vX.Y.Z`.
