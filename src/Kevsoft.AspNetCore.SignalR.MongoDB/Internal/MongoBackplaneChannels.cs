using System.Runtime.CompilerServices;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoBackplaneChannels
{
    private readonly string _prefix;

    public MongoBackplaneChannels(string prefix, string serverName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(serverName);

        _prefix = prefix;
        StreamId = prefix;
        All = prefix + ":all";
        GroupManagement = prefix + ":internal:groups";
        ReturnResults = prefix + ":internal:return:" + serverName;
    }

    public string StreamId { get; }

    public string All { get; }

    public string GroupManagement { get; }

    public string ReturnResults { get; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string Connection(string connectionId)
    {
        return _prefix + ":connection:" + connectionId;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string Group(string groupName)
    {
        return _prefix + ":group:" + groupName;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string User(string userId)
    {
        return _prefix + ":user:" + userId;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public string Ack(string serverName)
    {
        return _prefix + ":internal:ack:" + serverName;
    }
}
