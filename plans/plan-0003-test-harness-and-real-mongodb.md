# Plan 0003 - Test harness and real MongoDB

## Goal

Prove the lifetime manager is SignalR-compatible with a fast in-memory harness, and establish Docker/Testcontainers-based real MongoDB testing before either MongoDB transport is treated as complete.

## Dependencies

- Plan 0002 complete.
- The core manager depends on an injectable backplane abstraction.
- The test project can restore or reference `Microsoft.AspNetCore.SignalR.Specification.Tests`.

## Real MongoDB test requirement

Transport behavior must be validated against an actual MongoDB server, preferably started by Testcontainers in Docker.

- Do not rely only on mocked `IMongoCollection<T>` or the in-memory backplane for transport correctness.
- The in-memory backplane is a fast design/specification harness for core `HubLifetimeManager` behavior.
- Each MongoDB transport must also run specification-style scale-out tests against a real MongoDB instance before that transport is accepted.
- Docker-dependent tests can be categorized so developers can run fast tests separately, but CI/release verification should run the real MongoDB suites.
- Test logs should include MongoDB container output and connection details with secrets redacted.

## In-memory backplane

Create a test-only in-memory backplane similar to Redis' `TestRedisServer`:

- Stores channel subscriptions in process.
- Publishes envelopes to all subscribers for the logical channel.
- Preserves asynchronous delivery behavior enough to catch ordering and ack issues.
- Supports all channel types used by the manager:
  - all
  - group
  - user
  - connection
  - group management
  - ack
  - return results
- Does not depend on MongoDB.

This fake should exercise the same manager paths that MongoDB transports will use. Avoid test-only shortcuts that bypass the protocol, ack, or client-result code.

## Specification tests

Create a test class based on:

```csharp
ScaleoutHubLifetimeManagerTests<TBackplane>
```

Implement:

- `CreateBackplane()`
- `CreateNewHubLifetimeManager()`
- `CreateNewHubLifetimeManager(TBackplane backplane)`

Use a helper factory that creates `MongoDbHubLifetimeManager<Hub>` with:

- null/test logger
- default options
- default hub protocol resolver including JSON and another non-JSON hub protocol where available
- in-memory backplane injected through options/factory/service registration

This suite should run without Docker and should be the earliest feedback loop for core manager correctness.

## MongoDB container infrastructure

Add shared test infrastructure for real MongoDB:

- Use Testcontainers for .NET if it is compatible with the project, otherwise provide a small Docker-based fixture.
- Create an isolated database per test class or per test to avoid cross-test contamination.
- Generate unique collection names for parallel-safe tests.
- Expose helpers to create:
  - standalone MongoDB for tailable-await tests.
  - single-node replica set MongoDB for change-stream tests.
- Provide helpers for capped collection creation, TTL index creation, and cleanup.
- Skip or mark Docker-dependent tests only when Docker is unavailable; do not silently pass them.

## Real MongoDB specification-style tests

For each implemented transport, create a specification-style test class that uses the real MongoDB fixture:

- `TBackplane` should represent the MongoDB fixture/backplane configuration, not an in-memory fake.
- `CreateNewHubLifetimeManager(TBackplane backplane)` should create managers that share the same real MongoDB collection.
- Run the same scale-out scenarios for all/group/user/connection sends, remote group add/remove, and client results.
- Keep transport-specific setup in fixtures so the test assertions stay aligned with Microsoft's specification tests.

## Additional behavior tests

Add targeted tests not fully covered by the specification suite:

- Local `SendConnectionAsync` writes directly and does not require the backplane.
- Broadcast/group/user messages are delivered through loopback exactly once.
- Group add/remove for remote connections waits for ack.
- Ack timeout propagates a failure/cancellation.
- Client result invocation works across two managers.
- Malformed envelopes are logged and do not stop future message processing.
- Disposing a manager unsubscribes and cancels pending operations.

## Acceptance criteria

- The full scale-out specification suite passes against the in-memory backplane.
- Core behavior tests pass without MongoDB or Docker.
- Real MongoDB container fixtures exist before tailable-await or change-stream implementation is accepted.
- Each MongoDB transport has a required real-MongoDB specification-style suite once implemented.
- The test setup provides both fast local feedback and real backend verification.
- Any public option/DI seams needed for testability are finalized before MongoDB transport implementation begins.
