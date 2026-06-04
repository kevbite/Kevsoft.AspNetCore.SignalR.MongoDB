# Plan 0009 - Optional MessagePack envelope extension

## Goal

Add a separate optional extension package that can encode MongoDB backplane envelopes with MessagePack for applications that want a compact binary envelope format, without making MessagePack a dependency of the core package.

## Dependencies

- Core BSON-first protocol from Plan 0002 is implemented.
- `IBackplaneEnvelopeSerializer` or equivalent serializer abstraction is stable and replaceable through DI/options.
- Both MongoDB transports can consume envelopes through the serializer abstraction rather than directly depending on BSON-specific code paths.

## Package shape

Create a separate package, for example:

- `Kevsoft.AspNetCore.SignalR.MongoDB.MessagePack`

This package should:

- reference the core `Kevsoft.AspNetCore.SignalR.MongoDB` package.
- reference `MessagePack`.
- provide a MessagePack implementation of the backplane envelope serializer.
- provide DI extension methods to opt in explicitly.
- avoid changing default behavior for existing BSON users.

## Public API

Consider extension methods such as:

```csharp
services.AddSignalR()
    .AddMongoDb(options => { ... })
    .AddMongoDbMessagePackEnvelope();
```

or an options callback:

```csharp
services.AddSignalR()
    .AddMongoDb(options =>
    {
        options.UseMessagePackEnvelope();
    });
```

Choose the API that composes best with the final DI design from Plan 0006.

## Compatibility rules

- The serializer must include enough versioning information to reject unsupported envelope versions clearly.
- BSON and MessagePack envelope formats do not need to be wire-compatible with each other unless a migration strategy is explicitly designed.
- A deployment should use one envelope format per shared MongoDB backplane collection.
- If mixed formats are supported later, the envelope document must include an encoding discriminator.

## Tests

Add tests that prove:

- MessagePack serializer round-trips every envelope kind.
- BSON and MessagePack extension packages can be referenced independently.
- DI registration replaces only the envelope serializer and does not replace transports or lifetime manager behavior.
- Real MongoDB integration tests pass when the MessagePack serializer is enabled.
- A clear error is reported if a BSON-configured node reads a MessagePack envelope, unless mixed-format support is implemented.

## Acceptance criteria

- The core package remains free of any MessagePack dependency.
- MessagePack support is opt-in through a separate package.
- Applications can choose the MessagePack envelope serializer without changing hub code.
- Real MongoDB tests pass with the optional serializer enabled.
