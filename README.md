# Kevsoft.AspNetCore.SignalR.MongoDB

`Kevsoft.AspNetCore.SignalR.MongoDB` is a MongoDB-backed scale-out provider for ASP.NET Core SignalR. It is being built to support two MongoDB transport modes: change streams for replica sets/sharded clusters, and tailable-await cursors over capped collections for standalone-friendly deployments.

> Status: early implementation. The core lifetime manager and BSON protocol are in place, and the in-memory SignalR scale-out specification suite is passing. Real MongoDB transports and final DI extensions are under active development.

## Goals

- Provide a simple SignalR backplane for applications already using MongoDB.
- Support both `ChangeStreams` and `TailableAwait` transports.
- Store transient in-process checkpoints:
  - change-stream resume tokens.
  - tailable cursor positions.
- Keep the core package BSON-first while preserving extension points for optional serializers in separate packages.
- Validate behavior with Microsoft’s SignalR specification tests and real MongoDB integration tests.

## Transport modes

| Mode | Best for | MongoDB requirement | Notes |
| --- | --- | --- | --- |
| `ChangeStreams` | Modern production MongoDB clusters | Replica set or sharded cluster | Preferred transport when available. Uses resume tokens for in-process reconnects and TTL cleanup for message documents. |
| `TailableAwait` | Standalone MongoDB or capped-collection deployments | Capped collection | Uses a tailable-await cursor. Collection overflow can drop old messages during bursts. |

Both modes treat SignalR backplane messages as ephemeral. Cold starts begin from live messages and do not replay historical documents into current hub connections.

## Intended setup

The final consumer experience will be an ASP.NET Core DI extension that registers the MongoDB lifetime manager and selected transport. The intended shape is:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDB(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("MongoDB");
        options.DatabaseName = "my_app";
        options.CollectionName = "signalr_messages";
        options.ChannelPrefix = "my-app";
        options.TransportMode = MongoDbSignalRTransportMode.ChangeStreams;
    });
```

For tailable-await:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDB(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("MongoDB");
        options.DatabaseName = "my_app";
        options.CollectionName = "signalr_capped_messages";
        options.TransportMode = MongoDbSignalRTransportMode.TailableAwait;
        options.TailableCollectionSizeBytes = 64 * 1024 * 1024;
        options.TailableAwaitMaxAwaitTime = TimeSpan.FromSeconds(1);
    });
```

The DI extension is planned as part of the consumer-experience work. Until that lands, the package is not ready for production consumption.

## Key options

| Option | Purpose |
| --- | --- |
| `ConnectionString` | MongoDB connection string used when no custom client factory is provided. |
| `DatabaseName` | Database containing the backplane collection. |
| `CollectionName` | Collection used for SignalR backplane message documents. |
| `TransportMode` | Selects `ChangeStreams` or `TailableAwait`. |
| `MongoClientFactory` | Allows applications to provide an existing `IMongoClient`. |
| `ChannelPrefix` | Isolates applications or hubs sharing the same MongoDB collection. |
| `AckTimeout` | Timeout for remote group-management acknowledgements. |
| `TailableAwaitMaxAwaitTime` | Max idle server wait for tailable-await cursor operations. |
| `TailableCollectionSizeBytes` | Capped collection size for the tailable-await transport. |
| `MessageTtl` | Retention period for cleanup-capable transports. |
| `CheckpointStore` | Stores transient in-process cursor checkpoints. |
| `CreateCollectionIfMissing` | Allows startup collection creation. |
| `CreateIndexes` | Allows startup index creation. |
| `RunCollectionSetupOnStartup` | Runs collection/index setup during startup. |

## Operational notes

- Change streams require MongoDB to run as a replica set or sharded cluster.
- Tailable-await requires a capped collection.
- Checkpoints are not a durable replay mechanism. They are only used to reduce gaps during in-process cursor interruptions.
- MongoDB TTL cleanup is best-effort; documents can remain briefly after `MessageTtl`.
- MongoDB inserts cannot report how many SignalR servers are subscribed. The implementation uses separate presence tracking for remote connection checks.

## Development

Run the full fast test suite:

```bash
dotnet test Kevsoft.AspNetCore.SignalR.MongoDB.slnx --configuration Release --nologo
```

Real MongoDB transport tests are expected to run against Docker/Testcontainers. Tailable-await tests use a standalone MongoDB container; change-stream tests use a single-node replica set container.
