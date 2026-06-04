# Plan 0007 - Integration, reliability, and release hardening

## Goal

Turn the implementation into a reliable, documented, package-ready .NET 10 library.

## Dependencies

- Plans 0001 through 0006 are complete.
- Both MongoDB transports exist or have a documented staged release decision.
- Real MongoDB test infrastructure was introduced in Plan 0003 and is already used by transport tests.

## Integration test infrastructure

Use a real MongoDB server for all transport verification:

- Prefer Testcontainers for repeatable CI tests.
- Change streams require MongoDB running as a replica set.
- Tailable-await can run against standalone MongoDB but still needs capped collection setup.
- Mark or categorize Docker-dependent tests so local fast tests remain usable, but do not treat a transport as verified unless the real MongoDB suite has passed.

Test matrix:

- in-memory specification suite: always runs.
- tailable-await specification-style and integration suite: Docker/Mongo required.
- change-stream specification-style and integration suite: Docker/Mongo replica set required.
- DI/options tests: always runs.
- protocol/unit tests: always runs.

## End-to-end scenarios

Add ASP.NET Core end-to-end tests similar to Redis:

- two app servers using the same MongoDB backplane.
- WebSockets and fallback transports where feasible.
- JSON and non-JSON hub protocols, including a binary SignalR hub protocol if that test dependency is added.
- broadcast, group, user, specific connection, multiple groups/users.
- group names and user ids with special characters treated literally.
- client result invocation across servers.
- reconnect/cursor interruption behavior.

## Reliability checks

Cover:

- MongoDB unavailable at startup.
- MongoDB unavailable after startup.
- watcher/tailable cursor interruption.
- duplicate envelope handling if a reconnect re-reads the last document.
- malformed payload logging.
- ack timeout behavior.
- graceful application shutdown.
- high-volume bursts relative to capped collection size.
- change-stream resume token expiration/history lost fallback.

## Performance and operational guidance

Measure or document:

- expected additional latency for tailable-await loopback.
- recommended capped collection size formula based on message rate and outage tolerance.
- TTL retention guidance for change-stream collection.
- write concern tradeoffs.
- indexes required for cleanup and filtering.
- limitations compared with Redis pub/sub.

## Packaging

Prepare NuGet packaging:

- package metadata, description, repository URL, license, tags.
- XML docs enabled.
- nullable warnings treated consistently.
- SourceLink if desired.
- public API review file if the project adopts one.
- versioning strategy.
- changelog or release notes.

## CI readiness

Prepare the command set that GitHub Actions will later run:

- restore.
- build.
- unit/spec tests.
- real MongoDB integration tests in an environment with Docker.
- pack.

Keep integration failures actionable by logging MongoDB container output and driver exceptions.

The actual GitHub Actions workflows, branch/tag triggers, artifact publishing, and nuget.org deployment are covered in Plan 0008.

## Acceptance criteria

- Fast tests pass without Docker.
- Real MongoDB specification-style and integration tests pass in a clean environment with Docker.
- Package can be packed and consumed by a sample app.
- Documentation explains supported modes, requirements, defaults, and failure semantics.
- Known limitations are explicit rather than hidden in implementation details.
- The repository is ready for the CI/CD automation in Plan 0008.
