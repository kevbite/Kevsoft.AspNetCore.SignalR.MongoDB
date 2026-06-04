namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal enum MongoBackplaneMessageKind
{
    Invocation,
    GroupCommand,
    Ack,
    Completion
}
