namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal readonly record struct MongoCompletion(string ProtocolName, byte[] CompletionMessage);
