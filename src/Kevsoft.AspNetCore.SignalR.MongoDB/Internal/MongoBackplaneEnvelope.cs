namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal readonly record struct MongoBackplaneEnvelope(
    MongoBackplaneMessageKind Kind,
    string Channel,
    object Payload,
    string? ServerId = null,
    DateTimeOffset? CreatedAt = null);
