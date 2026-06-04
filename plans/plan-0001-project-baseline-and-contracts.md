# Plan 0001 - Project baseline and public contracts

## Goal

Establish the library shape, dependencies, public options, and core contracts before implementing the SignalR lifetime manager or MongoDB transports.

## Current state

- Repository is a skeleton with a library project at `src/Kevsoft.AspNetCore.SignalR.MongoDB` and a test project at `tests/Kevsoft.AspNetCore.SignalR.MongoDB.Tests`.
- The library currently targets `net8.0;net9.0;net10.0`, has nullable enabled, and references `Microsoft.AspNetCore.SignalR.Core`.
- The test project targets `net10.0` and only contains a placeholder xUnit test.
- The Redis implementation in ASP.NET Core is the best architectural reference:
  - `AddStackExchangeRedis(...)` extension overloads configure options and register `HubLifetimeManager<>`.
  - `RedisHubLifetimeManager<THub>` owns local connection/group/user state and delegates cross-server delivery to a backplane.
  - Redis uses a protocol envelope carrying `SerializedHubMessage`, group-management commands with acks, and client-result return channels.
- `Microsoft.AspNetCore.SignalR.Specification.Tests` provides the base scale-out behavior suite. Its scale-out tests are designed to run against an in-memory backplane fake, not a live service.

## Decisions

1. **Target frameworks**
   - Recommended: make `net10.0` the primary target for the first implementation.
   - Keep `net8.0;net9.0` package compatibility is a goal from the start;

2. **MongoDB package baseline**
   - Add `MongoDB.Driver` to the library.
   - Use MongoDB's native BSON support for internal envelope documents instead of adding a second binary serializer dependency.
   - Verify the current `Microsoft.AspNetCore.SignalR.Specification.Tests` NuGet package restores for `net10.0`. If not, temporarily reference/copy the specification source from `/home/kevin/dev/dotnet/aspnetcore/src/SignalR/server/Specification.Tests/src` into tests with attribution preserved.

3. **Backplane semantics**
   - Treat the MongoDB collection as an ephemeral SignalR bus, not a durable queue.
   - Do not replay old messages on cold start. A new process should start from "now" for both change streams and tailable cursors.
   - Position storage is only for transient in-process recovery after cursor reconnects. The initial implementation can be in-memory only.

4. **Loopback delivery**
   - The publishing server must receive its own broadcast/group/user messages through the backplane loop, matching Redis behavior.
   - Do not filter self-authored envelopes for all/group/user channels.
   - Do not also write those messages directly to local clients, or duplicate delivery becomes possible.

## Proposed public surface

### Options

Create `MongoDbSignalROptions`:

- `string? ConnectionString`
- `string? DatabaseName`
- `string CollectionName = "signalr_messages"`
- `MongoDbSignalRTransportMode TransportMode = MongoDbSignalRTransportMode.ChangeStreams`
- `Func<IServiceProvider, IMongoClient>? MongoClientFactory`
- `Action<MongoClientSettings>? ConfigureClientSettings`
- `string? ChannelPrefix`
- `TimeSpan AckTimeout`
- `TimeSpan TailableAwaitMaxAwaitTime`
- `int TailableCollectionSizeBytes`
- `TimeSpan MessageTtl`
- `IMessageCheckpointStore? CheckpointStore`
- transport-specific option groups if the single class becomes too large:
  - `MongoDbChangeStreamOptions`
  - `MongoDbTailableAwaitOptions`

### Transport mode enum

```csharp
public enum MongoDbSignalRTransportMode
{
    ChangeStreams,
    TailableAwait
}
```

### Checkpoint contracts

Create storage contracts that make the transient semantics explicit:

```csharp
public interface IMessageCheckpointStore
{
    ValueTask<MongoDbBackplaneCheckpoint?> GetAsync(string streamId, CancellationToken cancellationToken);
    ValueTask SetAsync(string streamId, MongoDbBackplaneCheckpoint checkpoint, CancellationToken cancellationToken);
    ValueTask ClearAsync(string streamId, CancellationToken cancellationToken);
}
```

`MongoDbBackplaneCheckpoint` should support:

- change stream resume token as `BsonDocument` or `BsonValue`.
- tailable cursor position as last seen document id/sequence/time, with caveats documented.
- timestamp of when the checkpoint was stored.

Implement `InMemoryMessageCheckpointStore` now. Do not add a durable store in the first iteration.

## Internal contracts to define

Introduce a transport abstraction so the lifetime manager is not coupled directly to MongoDB:

- `IMongoSignalRBackplane`
  - publish envelope
  - subscribe to logical channels
  - start/stop/dispose
- `IMongoMessagePublisher`
- `IMongoMessageSubscriber`
- `IBackplaneEnvelopeSerializer`
- `MongoBackplaneChannels`

This abstraction is required so the specification tests can run against an in-memory fake before any real MongoDB transport exists.

## Open questions to confirm

- Should the NuGet package support only .NET 10 initially, or keep `net8.0` and `net9.0` multi-targeting? Keep all 10 to 8 net framework support.
- What should the public package/API name be: `Kevsoft.AspNetCore.SignalR.MongoDB`, `AddMongoDB`, `AddMongoDb`, or `AddMongoDbSignalR`? Public package should be Kevsoft.AspNetCore.SignalR.MongoDB
- Is the MongoDB message collection allowed to be created/configured by the library, or should production users create it themselves? We should configure it by default the collection, however, this can be configurable via the Options the user can supply. Also we should build this in to a class so that the user can consume that class once regiestered in the IoC container and run it them selves too. Thus then can run this on CI/CD as well as on startup. We should make sure we document how the use can do this.
- What is the path to the older MongoDB SignalR project? It was not discoverable from the provided paths. (The code is here /home/kevin/dev/kevbite/Mongo.SignalR.Backplane)
- Should future durable checkpoint stores be considered a supported extension point now, or deferred until replay semantics are fully designed? It should be able to be replace with the options that they pass in if required but should fall back to inmemory.

## Acceptance criteria

- Library and test projects restore on .NET 10.
- Public option and checkpoint contracts are in place with XML docs.
- Dependency choices are documented in the project files.
- The transient/no-cold-replay invariant is documented in code comments or README notes.
- No MongoDB transport code is implemented before the core contracts and testability seams exist.
