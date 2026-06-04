# Plan 0002 - Core lifetime manager and protocol

## Goal

Build the SignalR `HubLifetimeManager<THub>` implementation and internal envelope protocol against an abstract backplane, following the Redis implementation as closely as practical.

## Dependencies

- Plan 0001 complete.
- Public options and internal transport contracts exist.
- Test project can reference the library and SignalR test/spec packages.

## Core implementation

Create `MongoDbHubLifetimeManager<THub> : HubLifetimeManager<THub>, IDisposable` or `IAsyncDisposable`.

Responsibilities:

- Maintain local `HubConnectionStore` for active connections.
- Maintain group and user subscription state equivalent to Redis' `RedisSubscriptionManager`.
- Generate a server name/id for ack and client-result return channels.
- Subscribe to:
  - all channel
  - group management channel
  - ack channel for this server
  - return-results channel for this server
  - per-connection channels
  - per-group channels
  - per-user channels
- Publish invocations through the abstract backplane.
- Write local connection sends directly only for `SendConnectionAsync` when the connection is local.
- Use the backplane loop for broadcast/group/user delivery, including self-authored messages.

## Protocol model

Create a MongoDB-neutral envelope protocol equivalent to Redis:

- invocation envelope
  - logical channel
  - excluded connection ids
  - serialized hub message map keyed by hub protocol name
  - optional invocation id for client results
  - optional return channel
- group command envelope
  - command id
  - origin server
  - action: add/remove
  - group name
  - connection id
- ack envelope
  - command id
  - target server channel
- completion envelope
  - protocol name
  - serialized completion payload

Prefer a compact binary payload stored in the MongoDB document, rather than storing protocol-specific JSON fields. This keeps behavior consistent across JSON and MessagePack hub protocols.

## Serializer

Port/adapt the Redis approach:

- `DefaultHubMessageSerializer` equivalent for supported hub protocols.
- `MongoDbBackplaneProtocol` for envelope read/write.
- Keep protocol parsing strict and fail fast on malformed envelopes.
- Add tests for round-tripping each envelope type.

## Group management and acknowledgements

Implement an ack handler equivalent to Redis:

- `CreateAck(id)` returns a task.
- `TriggerAck(id)` completes it.
- expired acks cancel or timeout.
- disposing the manager cancels pending acks.

`AddToGroupAsync` and `RemoveFromGroupAsync` behavior:

- If connection is local, update local group state directly.
- If connection is remote/unknown, publish a group command and wait for an ack.
- The server holding the connection applies the group change and publishes the ack.

## Client results

Support `InvokeConnectionAsync<T>`, `SetConnectionResultAsync`, and `TryGetReturnType`:

- Reuse `ClientResultsManager` from accessible shared source if available, or implement a local equivalent.
- For remote connection invokes, include invocation id and return channel in the invocation envelope.
- The server with the connection forwards completion messages to the return channel.
- The origin server completes the pending invocation.

## Error handling and lifecycle

- Start subscriptions lazily when the first connection is added, mirroring Redis' connection pattern.
- Surface connection/startup errors instead of silently swallowing them.
- Log message-processing failures but keep the subscriber running when possible.
- Ensure cancellation tokens are passed through all async operations.
- Dispose subscriptions, pending acks, pending client results, and transport resources.

## Acceptance criteria

- Core lifetime manager compiles against an abstract transport.
- Unit tests cover protocol serialization, ack handling, group subscription management, and disposal.
- No direct MongoDB driver dependency is required by tests for the core manager.
- The code preserves Redis-equivalent loopback semantics for all/group/user sends.
