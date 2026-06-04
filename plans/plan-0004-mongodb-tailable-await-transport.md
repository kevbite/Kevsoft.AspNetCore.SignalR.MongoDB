# Plan 0004 - MongoDB tailable-await transport

## Goal

Implement the MongoDB capped-collection transport that publishes backplane envelopes as documents and consumes them with `CursorType.TailableAwait`.

## Dependencies

- Plan 0003 complete and passing.
- Envelope document schema is finalized.
- Core manager works with any `IMongoSignalRBackplane` implementation.
- Real MongoDB test fixtures are available for standalone/capped-collection testing.

## Collection model

Use a capped collection for tailable cursors.

Document shape:

- `_id`: MongoDB generated id or an explicit id.
- `streamId`: channel namespace/prefix for this hub.
- `channel`: logical channel name.
- `kind`: invocation, group command, ack, completion.
- `payload`: binary protocol payload.
- `createdAtUtc`: message timestamp.
- `serverId`: publishing server id for diagnostics only.
- optional `sequence`: only if a safe monotonic sequence strategy is introduced.

Collection requirements:

- Create capped collection when configured to do so.
- Configurable capped size.
- Seed/sentinel document support, because tailing an empty capped collection can be problematic.
- Document that capped collection overflow can drop messages under bursts.

## Publishing

`MongoDbTailableAwaitBackplane` should:

- serialize the envelope through the shared protocol.
- insert one document per published envelope.
- return only after MongoDB acknowledges the insert according to configured write concern.
- not deliver messages directly to local subscribers; delivery comes from the tailable consumer loop.

## Consuming

Create a background subscriber loop:

- Query the capped collection by stream/channel as appropriate.
- Use `FindOptions.CursorType = CursorType.TailableAwait`.
- Configure `MaxAwaitTime` from options.
- Continue iterating until cancellation/disposal.
- Dispatch matching documents to local channel subscribers.
- Store the last observed transient checkpoint after successful dispatch.

## Checkpoint behavior

Important invariant: do not replay old messages on process cold start.

- On first start, begin tailing at the end/live position.
- During an in-process cursor fault/reconnect, use the in-memory checkpoint to reduce gaps if safe.
- Avoid relying solely on `ObjectId > lastSeen` for correctness across multiple writers because ObjectIds are client-generated and can be affected by clock skew.
- If a reliable monotonic sequence is not implemented, prefer reconnecting live and document possible message loss during outages.

## Latency and tuning

Because broadcasts and group/user sends loop through MongoDB even on the publishing server:

- `TailableAwaitMaxAwaitTime` directly affects local delivery latency.
- Defaults should favor low latency without excessive polling.
- Add tests or benchmarks to detect unexpectedly high local loopback delay.

## Tests

Unit tests with mocked/fake Mongo abstractions are useful for edge cases, but they are not sufficient for transport acceptance:

- collection creation decisions.
- document schema serialization.
- insert options and write concern.
- checkpoint store updates after dispatch.
- cancellation/disposal behavior.

Integration tests with real MongoDB:

- Run in Docker/Testcontainers against an actual MongoDB server.
- Start a capped collection.
- Publish from manager 1 and receive on manager 2.
- Verify same-server loopback delivery.
- Verify group/user/connection behavior with the specification-style scenarios.
- Verify reconnect after cursor interruption does not duplicate messages.
- Verify collection overflow behavior is documented, not hidden.

## Acceptance criteria

- Tailable-await transport passes the existing in-memory specification tests via the same manager paths.
- Real MongoDB specification-style and integration tests pass for all core send/group/user/client-result scenarios.
- Startup validates capped collection requirements and reports actionable errors.
- Checkpoint behavior is transient and does not replay stale messages after cold start.
