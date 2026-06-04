namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal static class MongoBackplaneDocumentFields
{
    public const string Id = "_id";
    public const string StreamId = "streamId";
    public const string Version = "version";
    public const string CreatedAtUtc = "createdAtUtc";
    public const string ConnectionId = "connectionId";
    public const string ServerId = "serverId";
    public const string ExpiresAtUtc = "expiresAtUtc";
}
