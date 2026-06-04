using Microsoft.AspNetCore.SignalR;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal readonly record struct MongoInvocation(
    SerializedHubMessage Message,
    IReadOnlyList<string>? ExcludedConnectionIds,
    string? InvocationId = null,
    string? ReturnChannel = null);
