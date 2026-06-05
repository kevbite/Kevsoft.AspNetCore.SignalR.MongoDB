namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal readonly record struct MongoBackplaneEnvelope(
    string Channel,
    MongoBackplanePayload Payload,
    string? ServerId = null,
    DateTimeOffset? CreatedAt = null);
