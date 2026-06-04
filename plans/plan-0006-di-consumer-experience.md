# Plan 0006 - DI and consumer experience

## Goal

Make the library easy to consume from ASP.NET Core applications using familiar SignalR builder extensions and validated options.

## Dependencies

- Core manager and at least one MongoDB transport are implemented.
- Public options from Plan 0001 are stable.

## Extension methods

Add extension methods in `Microsoft.Extensions.DependencyInjection`, mirroring Redis:

```csharp
services.AddSignalR()
    .AddMongoDb();

services.AddSignalR()
    .AddMongoDb(connectionString);

services.AddSignalR()
    .AddMongoDb(options =>
    {
        options.ConnectionString = "...";
        options.DatabaseName = "app";
        options.TransportMode = MongoDbSignalRTransportMode.ChangeStreams;
    });

services.AddSignalR()
    .AddMongoDb(connectionString, options =>
    {
        options.DatabaseName = "app";
    });
```

Consider whether the public method should be `AddMongoDB`, `AddMongoDb`, or `AddMongoDbSignalR`. Choose one primary spelling and keep aliases only if they add real value.

## Service registration

Register:

- `HubLifetimeManager<>` as `MongoDbHubLifetimeManager<>`.
- `IOptions<MongoDbSignalROptions>` and validators.
- `IMessageCheckpointStore` defaulting to `InMemoryMessageCheckpointStore`.
- `IMongoClient` or a factory, depending on options.
- transport implementation selected by `TransportMode`.
- serializers/protocol helpers.
- hosted/background services only if needed; prefer manager-owned lifecycle if it matches SignalR patterns better.

## Options validation

Validate at startup:

- connection string or client factory is provided.
- database name is provided.
- collection name is valid.
- capped collection size is positive for tailable mode.
- change-stream mode is not configured with tailable-only settings unless harmless.
- ack timeout and await times are positive.
- message TTL is positive when TTL cleanup is enabled.

Use .NET 10 options validation patterns:

- `IValidateOptions<TOptions>`
- `ValidateOnStart` where appropriate
- source-generated validation only if it fits cleanly and avoids unnecessary complexity.

## Consumer documentation

Add README examples:

- minimal setup with change streams.
- tailable-await setup for standalone MongoDB.
- configuring collection creation and TTL/capped size.
- replacing the checkpoint store.
- explaining no-cold-replay semantics.
- explaining MongoDB deployment requirements for change streams.

## DI tests

Add tests mirroring Redis' DI tests:

- extension overloads register `HubLifetimeManager<>`.
- connection string maps to options.
- configure callback applies.
- default transport mode is set.
- custom `IMongoClient` factory is used.
- invalid options fail validation with actionable messages.

## Acceptance criteria

- A minimal ASP.NET Core app can consume the library with one fluent `AddSignalR().AddMongoDb(...)` call.
- DI tests cover every extension overload.
- Options validation catches common misconfiguration before the first hub connection.
- README examples match the implemented API exactly.
