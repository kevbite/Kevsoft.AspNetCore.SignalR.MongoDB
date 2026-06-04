# Kevsoft.AspNetCore.SignalR.MongoDB

`Kevsoft.AspNetCore.SignalR.MongoDB` is a MongoDB-backed scale-out provider for ASP.NET Core SignalR. It is being built to support two MongoDB transport modes: change streams for replica sets/sharded clusters, and tailable-await cursors over capped collections for standalone-friendly deployments.

## Setup

Register MongoDB scale-out from the SignalR server builder:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("MongoDB");
        options.DatabaseName = "my_app";
        options.CollectionName = "signalr_messages";
        options.ChannelPrefix = "my-app";
        options.TransportMode = MongoDbSignalRTransportMode.ChangeStreams;
    });
```

If the connection string includes the database name, the connection string overload can infer `DatabaseName`:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb("mongodb://localhost:27017/my_app");
```

For tailable-await:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("MongoDB");
        options.DatabaseName = "my_app";
        options.CollectionName = "signalr_capped_messages";
        options.TransportMode = MongoDbSignalRTransportMode.TailableAwait;
        options.TailableCollectionSizeBytes = 64 * 1024 * 1024;
        options.TailableAwaitMaxAwaitTime = TimeSpan.FromSeconds(1);
    });
```

## Transport modes

| Mode | Best for | MongoDB requirement | Notes |
| --- | --- | --- | --- |
| `ChangeStreams` | Modern production MongoDB clusters | Replica set or sharded cluster | Preferred transport when available. Uses resume tokens for in-process reconnects and TTL cleanup for message documents. |
| `TailableAwait` | Standalone MongoDB or capped-collection deployments | Capped collection | Uses a tailable-await cursor. Collection overflow can drop old messages during bursts. |

Both modes treat SignalR backplane messages as ephemeral. Cold starts begin from live messages and do not replay historical documents into current hub connections.


## Key options

| Option | Purpose |
| --- | --- |
| `ConnectionString` | MongoDB connection string used when no custom client factory is provided. |
| `DatabaseName` | Database containing the backplane collection. |
| `CollectionName` | Collection used for SignalR backplane message documents. |
| `TransportMode` | Selects `ChangeStreams` or `TailableAwait`. |
| `MongoClientFactory` | Allows applications to provide an existing `IMongoClient`. |
| `ChannelPrefix` | Isolates applications sharing the same MongoDB collection. It is combined with the hub type so multiple hubs stay isolated. |
| `AckTimeout` | Timeout for remote group-management acknowledgements. |
| `TailableAwaitMaxAwaitTime` | Max idle server wait for tailable-await cursor operations. |
| `TailableCollectionSizeBytes` | Capped collection size for the tailable-await transport. |
| `MessageTtl` | Retention period for cleanup-capable transports. |
| `ConnectionPresenceTtl` | How long remote connection presence records are considered valid without a heartbeat. |
| `CheckpointStore` | Stores transient in-process cursor checkpoints. |
| `CreateCollectionIfMissing` | Allows startup collection creation. |
| `CreateIndexes` | Allows startup index creation. |
| `RunCollectionSetupOnStartup` | Runs collection/index setup during startup. |

## Operational notes

- Change streams require MongoDB to run as a replica set or sharded cluster. They are the preferred mode for production clusters.
- Tailable-await requires a capped collection. Size it for the largest outage or burst you need to tolerate: `average message bytes * messages per second * tolerated outage seconds`, plus headroom for indexes and message-size spikes.
- Checkpoints are not a durable replay mechanism. They are only used to reduce gaps during in-process cursor interruptions.
- MongoDB TTL cleanup is best-effort; documents can remain briefly after `MessageTtl`. Keep `MessageTtl` long enough for expected reconnect windows, but short enough to avoid unbounded collection growth.
- The change-stream transport creates TTL and stream/time indexes when index creation is enabled. Connection presence uses stream/connection and TTL indexes.
- MongoDB writes use the driver/database defaults for write concern unless applications configure the supplied `MongoClientSettings`. Stronger write concern can improve durability at the cost of latency.
- MongoDB inserts cannot report how many SignalR servers are subscribed. The implementation uses separate MongoDB presence records and heartbeats for remote connection checks.
- Unlike Redis pub/sub, MongoDB messages are stored briefly in collections. This helps cursor recovery but means capped-collection sizing, TTL cleanup, and collection permissions are operational concerns.

## Contributing

1. Issue
1. Fork
1. Hack!
1. Pull Request

