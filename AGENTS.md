# Agent development guide

This file captures project-specific guidance for humans and AI agents working on `Kevsoft.AspNetCore.SignalR.MongoDB`. Keep it updated whenever we learn a better implementation rule, testing pattern, or design constraint.

## Project direction

- Build an ASP.NET Core SignalR scale-out provider backed by MongoDB.
- Keep the core package BSON-first. Do not add MessagePack to the core package; preserve serializer extension points so a separate optional MessagePack package can be added later.
- Target modern .NET practices and keep the library compatible with the configured target frameworks in the project file.
- Follow the ASP.NET Core Redis scale-out provider patterns where they fit, but do not copy Redis semantics that MongoDB cannot provide.
- Prefer small, descriptive commits

## Design principles

- Keep transport-specific code thin. Shared responsibilities should live in shared MongoDB infrastructure:
  - document schema and BSON envelope serialization.
  - subscription registry and dispatch.
  - collection and index initialization.
  - connection presence tracking.
  - checkpoint persistence calls.
  - lifecycle, cancellation, disposal, and reconnect plumbing.
- `MongoDbHubLifetimeManager<THub>` should depend on abstractions, not concrete MongoDB transport implementations.
- Do not add broad catches or silent fallbacks. Log malformed documents and transport interruptions with actionable context.
- Preserve SignalR scale-out semantics:
  - broadcast, group, and user messages loop through the backplane, including on the publishing server.
  - local `SendConnectionAsync` writes directly and should not publish to MongoDB.
  - remote group add/remove waits for acknowledgements.
  - remote client results must support success, errors, cancellation, wrong returned type, and disconnection.

## MongoDB transport rules

- MongoDB inserts do not provide Redis-style subscriber counts. Real transports use separate connection-presence records and heartbeats for remote `InvokeConnectionAsync` existence checks.
- Cold start must begin from "now" and must not replay old backplane messages into current SignalR connections.
- Checkpoints are for transient in-process recovery only. Do not seed an initial cold-start cursor from a durable checkpoint store.
- Use a consistent top-level `streamId` to isolate hubs/apps sharing a collection.
- Treat malformed or foreign documents as per-document failures: log, skip, and keep the reader loop alive.
- Do not let slow channel handlers block the transport reader loop indefinitely, especially for ack and client-result channels.

### Tailable-await

- Use a capped collection and fail fast with an actionable error if an existing collection is not capped.
- Seed/sentinel documents are allowed, but reader code must skip them safely.
- Start live from a collection-end boundary. Prefer a natural-order end snapshot over timestamp-based filtering to avoid writer clock skew.
- `TailableAwaitMaxAwaitTime` caps idle server wait time; it is not expected to add that much latency to every message.

### Change streams

- Change streams require a replica set or sharded cluster.
- Watch inserts only; insert change events include `fullDocument`, so `UpdateLookup` is not needed for normal insert-only backplane messages.
- Resume with an in-process resume token after transient interruptions.
- Only fall back to a live restart when MongoDB reports the resume token is invalid, expired, or history is lost. Log that fallback as a warning.
- TTL cleanup is best-effort and should not be used for correctness.

## Testing expectations

- Run `dotnet test Kevsoft.AspNetCore.SignalR.MongoDB.slnx --configuration Release --nologo` after code changes.
- `global.json` pins the .NET 10 SDK feature band for deterministic local and CI builds. Update GitHub Actions SDK setup alongside it.
- Keep the Microsoft `ScaleoutHubLifetimeManagerTests<TBackplane>` suite passing against the in-memory backplane.
- MongoDB transports must have real MongoDB tests, preferably via Docker/Testcontainers:
  - standalone MongoDB for tailable-await.
  - single-node replica set MongoDB for change streams.
- Docker-dependent tests may be visibly skipped when Docker is unavailable, but they must not silently pass.
- Mark Docker/Testcontainers tests with `Category=Integration`. Use `--filter "Category!=Integration"` for the fast lane and the unfiltered test command for full MongoDB verification.
- Avoid test-only shortcuts that bypass the protocol, acknowledgement, client-result, or serializer paths.
- Transport startup should not return until the tailable cursor or change stream is actively watching; otherwise early publishes can be missed.
- If a reader cannot open before initial readiness is signaled, fail startup and stop background tasks instead of retrying forever behind a hung host startup.
- The public DI API spelling is `AddMongoDb(...)`. Keep README examples and tests aligned with that spelling.
- `ChannelPrefix` is an application prefix; the manager composes it with the hub type to prevent cross-hub leakage.

## Release expectations

- CI packages must pass fast tests, Docker-backed integration tests, package metadata validation, and local package-consumption validation.
- Release packages publish only from `v*.*.*` tags through the `nuget-production` GitHub Environment.
- Pass `/p:Version` and `/p:PackageVersion` explicitly in CI/release workflows so tag-driven package versions do not fall back to the project `VersionPrefix`.
- Keep `NUGET_API_KEY` scoped to the package and environment; do not expose publishing secrets to pull-request workflows.

## Documentation expectations

- Update `README.md` when public setup, options, transport behavior, or operational requirements change.
- Update this file when we discover a better rule or a project-specific pitfall.
- Keep examples aligned with implemented APIs. If an example shows planned API shape before implementation, label it clearly.
