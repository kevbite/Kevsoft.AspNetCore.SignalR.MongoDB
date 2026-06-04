# Plan 0008 - GitHub Actions and NuGet release

## Goal

Make the library fully deployable from GitHub using GitHub Actions, with validated packages published to nuget.org through a controlled CI/CD release workflow.

## Dependencies

- Plan 0007 complete.
- Package metadata and versioning strategy are defined.
- Fast tests and real MongoDB Docker/Testcontainers tests pass locally.
- A GitHub repository exists for the project.
- nuget.org publishing credentials or trusted publishing configuration are available.

## Workflow structure

Create separate workflows under `.github/workflows`:

- `ci.yml`
  - runs on pull requests and pushes to main.
  - restores, builds, runs fast tests, runs real MongoDB integration tests, and packs the library.
  - uploads `.nupkg`, `.snupkg`, test results, and coverage artifacts.
- `release.yml`
  - runs on version tags such as `v1.2.3`.
  - restores and builds from the tag.
  - runs the full test suite, including Docker-backed MongoDB tests.
  - packs deterministic release artifacts.
  - publishes packages to nuget.org.
  - creates or updates a GitHub Release with package artifacts and release notes.
- Optional `pr-validation.yml` only if CI becomes too slow and needs a smaller pull-request workflow.

## GitHub Actions requirements

Use current .NET and GitHub Actions practices:

- `actions/checkout`.
- `actions/setup-dotnet` with .NET 10 SDK.
- dependency caching for NuGet packages.
- `dotnet restore --locked-mode` once lock files are adopted.
- `dotnet build --configuration Release --no-restore`.
- `dotnet test --configuration Release --no-build`.
- `dotnet pack --configuration Release --no-build`.
- Docker-enabled runners for MongoDB/Testcontainers integration tests.
- workflow concurrency to cancel superseded pull-request runs.
- least-privilege workflow permissions.

## MongoDB in CI

The CI workflow must run the real MongoDB suites:

- Tailable-await tests can use standalone MongoDB.
- Change-stream tests need a single-node replica set.
- Prefer Testcontainers so tests own MongoDB startup and teardown.
- If Testcontainers is unreliable in GitHub Actions, define explicit MongoDB service containers and replica-set initialization steps.
- Capture MongoDB logs on failure.
- Do not publish packages unless the real MongoDB test suites pass.

## NuGet publishing

Publishing options:

1. Prefer nuget.org trusted publishing/OpenID Connect if available for this repository and package id.
2. Otherwise use a nuget.org API key stored as a GitHub Actions secret, scoped to only this package.

Release workflow rules:

- publish only from protected version tags.
- never publish from pull requests.
- use `--skip-duplicate` for idempotent reruns.
- publish symbol packages (`.snupkg`) with the main package.
- fail if package validation or tests fail.
- keep package artifacts available even if publishing fails.

## Versioning

Choose and implement one strategy:

- tag-driven versions, where tag `v1.2.3` produces package version `1.2.3`.
- Nerdbank.GitVersioning, MinVer, or another established versioning tool if automatic prerelease versions are desired.

Recommended staged behavior:

- Pull requests: build package with a non-published CI version.
- Main branch: optionally publish prerelease packages only if a prerelease feed is wanted.
- Version tags: publish stable packages to nuget.org.

## Package validation

Add release checks before publishing:

- package id, authors, description, tags, project URL, repository URL, license metadata.
- XML documentation included.
- symbols package generated.
- SourceLink works.
- package can be installed into a small sample app.
- public API surface is reviewed before stable releases.
- README/release notes describe MongoDB mode requirements and real-test coverage.

## Secrets and environments

Use GitHub Environments for release protection:

- `nuget-production` environment.
- required reviewers for stable release publishing.
- environment-scoped NuGet secret or trusted-publishing permission.
- no secrets exposed to pull-request workflows from forks.

## Acceptance criteria

- Pull requests run restore, build, fast tests, real MongoDB tests, and pack.
- Pushes to main run the same validation and retain package artifacts.
- Version tags publish validated `.nupkg` and `.snupkg` packages to nuget.org.
- Publishing is protected by GitHub environment rules and cannot run from untrusted PRs.
- GitHub Releases contain release notes and package artifacts.
- Documentation explains how maintainers cut and publish a release.
