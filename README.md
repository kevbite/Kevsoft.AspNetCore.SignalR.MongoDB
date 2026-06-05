# Kevsoft.AspNetCore.SignalR.MongoDB

`Kevsoft.AspNetCore.SignalR.MongoDB` is a MongoDB-backed scale-out provider for ASP.NET Core SignalR. It is being built to support two MongoDB transport modes: change streams for replica sets/sharded clusters, and tailable-await cursors over capped collections for standalone-friendly deployments.

## Setup

Register MongoDB scale-out from the SignalR server builder. Use the `Use*` methods on the options
object to configure each concern — they group related properties together so it is clear which
settings belong together and prevent mixing incompatible configuration.

### Connection string

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb(options =>
    {
        options.UseConnectionString(builder.Configuration.GetConnectionString("MongoDB")!, "my_app");
        options.UseChangeStreams();
        options.CollectionName = "signalr_messages";
        options.ChannelPrefix = "my-app";
    });
```

If the connection string already includes a database name, the single-argument overload infers it:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb("mongodb://localhost:27017/my_app");
```

### Existing `IMongoClient`

If your application already configures a `MongoClient`, pass a factory together with the database name:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb(options =>
    {
        options.UseMongoClient(sp => sp.GetRequiredService<IMongoClient>(), "my_app");
    });
```

### Existing `IMongoDatabase`

If your application registers an `IMongoDatabase` directly (e.g. via a shared infrastructure
registration), call `UseMongoDatabase` inside any `AddMongoDb` callback. Connection string and
database name are not required when using this path:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb(options =>
    {
        options.UseMongoDatabase(sp => sp.GetRequiredService<IMongoDatabase>());
    });
```

### Tailable-await (standalone MongoDB)

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb(options =>
    {
        options.UseConnectionString(builder.Configuration.GetConnectionString("MongoDB")!, "my_app");
        options.UseTailableAwait(o =>
        {
            o.CollectionSizeBytes = 64 * 1024 * 1024;
            o.MaxAwaitTime = TimeSpan.FromSeconds(1);
        });
        options.CollectionName = "signalr_capped_messages";
    });
```

### Resolving configuration from DI inside the callback

All `AddMongoDb` overloads have an `Action<IServiceProvider, MongoDbSignalROptions>` variant that
gives access to the root service provider. This is useful when the MongoDB settings live in a
registered service rather than being available at registration time:

```csharp
builder.Services
    .AddSignalR()
    .AddMongoDb((sp, options) =>
    {
        var cfg = sp.GetRequiredService<IConfiguration>();
        options.UseConnectionString(cfg.GetConnectionString("MongoDB")!, cfg["SignalR:Database"]!);
        options.ChannelPrefix = cfg["App:Name"];
    });
```

> **Note:** The `IServiceProvider` passed to these callbacks is the root (singleton-scope) provider.
> Do not resolve scoped services from it.

## Transport modes

| Mode | Best for | MongoDB requirement | Notes |
| --- | --- | --- | --- |
| `ChangeStreams` | Modern production MongoDB clusters | Replica set or sharded cluster | Preferred transport when available. Uses resume tokens for in-process reconnects and TTL cleanup for message documents. |
| `TailableAwait` | Standalone MongoDB or capped-collection deployments | Capped collection | Uses a tailable-await cursor. Collection overflow can drop old messages during bursts. |

Both modes treat SignalR backplane messages as ephemeral. Cold starts begin from live messages and do not replay historical documents into current hub connections.


## Key options

The `Use*` methods on `MongoDbSignalROptions` are the only way to configure connection and transport
properties (their setters are private). General options use regular property assignment.

### Connection methods

| Method | Properties set | Use when |
| --- | --- | --- |
| `UseConnectionString(string)` | `ConnectionString`, `DatabaseName` (inferred from URL) | Connection string includes the database name |
| `UseConnectionString(string, string, Action<MongoClientSettings>?)` | `ConnectionString`, `DatabaseName`, `ConfigureClientSettings` | Connection string and database name provided separately; optional driver settings |
| `UseMongoClient(factory, string)` | `MongoClientFactory`, `DatabaseName` | An `IMongoClient` is already registered in the container |
| `UseMongoDatabase(factory)` | `MongoDatabaseFactory` | An `IMongoDatabase` is already registered in the container |

### Transport methods

| Method | Properties set | Use when |
| --- | --- | --- |
| `UseChangeStreams(Action<MongoDbSignalRChangeStreamOptions>?)` | `TransportMode`, `MessageTtl` | Replica set or sharded cluster (recommended default) |
| `UseTailableAwait(Action<MongoDbSignalRTailableAwaitOptions>?)` | `TransportMode`, `TailableAwaitMaxAwaitTime`, `TailableCollectionSizeBytes` | Standalone MongoDB |

### `MongoDbSignalRChangeStreamOptions`

| Property | Default | Purpose |
| --- | --- | --- |
| `MessageTtl` | 1 day | Retention period for backplane documents; used to configure a TTL index. |

### `MongoDbSignalRTailableAwaitOptions`

| Property | Default | Purpose |
| --- | --- | --- |
| `MaxAwaitTime` | 1 second | Max idle server wait before the cursor returns an empty batch. |
| `CollectionSizeBytes` | 64 MB | Capped collection size. Size for peak throughput × tolerated outage window. |

### General options

| Option | Default | Purpose |
| --- | --- | --- |
| `CollectionName` | `signalr_messages` | Collection used for backplane message documents. |
| `ChannelPrefix` | `null` | Isolates applications sharing the same collection. Combined with the hub type. |
| `AckTimeout` | 30 s | Timeout for remote group-management acknowledgements. |
| `ConnectionPresenceTtl` | 2 min | How long remote connection presence records are considered valid without a heartbeat. |
| `CheckpointStore` | In-memory | Stores transient in-process cursor checkpoints. |
| `CreateCollectionIfMissing` | `true` | Creates the backplane collection on startup if it does not exist. |
| `CreateIndexes` | `true` | Creates required indexes on startup. |
| `RunCollectionSetupOnStartup` | `true` | Runs collection and index setup during startup. |

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

