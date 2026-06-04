# Plan 0005 - MongoDB change-stream transport

## Goal

Implement the MongoDB change-stream transport as the preferred modern backend for environments that support replica sets or sharded clusters.

## Dependencies

- Plan 0003 complete and passing.
- Shared envelope protocol and manager are stable.
- Real MongoDB replica-set test infrastructure exists from Plan 0003.

## Collection model

Use a normal MongoDB collection for published backplane documents.

Document shape should match the tailable transport:

- `_id`
- `streamId`
- `channel`
- `kind`
- `payload`
- `createdAtUtc`
- `serverId`

Add cleanup strategy:

- TTL index on `createdAtUtc`, configurable by `MessageTtl`.
- Optional capped collection support only if it does not conflict with change stream requirements.
- Startup validation for index creation if `CreateIndexes` is enabled.

## Publishing

`MongoDbChangeStreamBackplane` should:

- insert envelope documents into the configured collection.
- use configured write concern.
- rely on the change-stream watcher to deliver messages, including messages authored by the current server.
- avoid any self-filtering for broadcast/group/user channels.

## Consuming

Create a watcher loop:

- Use `IMongoCollection<T>.WatchAsync`.
- Filter to inserts for the configured stream/channel namespace.
- Deserialize the envelope payload and dispatch to local subscribers.
- Store the change stream resume token in the in-memory checkpoint store after successful dispatch.
- Reconnect on transient exceptions with bounded backoff.
- Stop promptly on cancellation/disposal.

## Resume semantics

Important invariant: do not replay old messages on process cold start.

- On cold start, begin from "now".
- During in-process reconnect, resume with the last in-memory resume token if available.
- If the token is invalid, expired, or history is lost, log a warning and restart from "now".
- Do not attempt to replay the collection history to catch up after process downtime.

## MongoDB requirements

Document clearly:

- Change streams require a replica set or sharded cluster.
- Standalone MongoDB instances should use the tailable-await transport instead.
- Resume tokens are only valid while the oplog contains the relevant history.
- Permissions must allow `watch` on the target collection/database.

## Tests

Unit tests are useful for edge cases, but they are not sufficient for transport acceptance:

- watch pipeline/filter creation.
- resume token persistence after successful dispatch.
- invalid-token fallback to live start.
- cancellation and disposal.
- TTL index creation logic.

Integration tests:

- Run in Docker/Testcontainers against an actual MongoDB single-node replica set.
- Verify all/group/user/connection messages across two managers.
- Verify same-server loopback delivery.
- Verify remote group add/remove ack behavior.
- Verify client-result return channel.
- Simulate watcher interruption and confirm reconnect behavior.

## Acceptance criteria

- Change-stream transport passes the shared manager behavior suite plus real MongoDB specification-style and integration tests.
- Cold starts do not replay historical messages.
- Invalid resume tokens are surfaced through logs and recover by starting live.
- Message cleanup prevents unbounded collection growth.
