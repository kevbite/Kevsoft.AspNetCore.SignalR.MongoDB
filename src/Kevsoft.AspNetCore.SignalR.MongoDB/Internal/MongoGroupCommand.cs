namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal readonly record struct MongoGroupCommand(
    int Id,
    string ServerName,
    GroupAction Action,
    string GroupName,
    string ConnectionId);
