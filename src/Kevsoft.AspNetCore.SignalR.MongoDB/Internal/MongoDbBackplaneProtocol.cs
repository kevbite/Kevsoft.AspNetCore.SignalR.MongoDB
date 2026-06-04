using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoDbBackplaneProtocol
{
    public MongoBackplaneEnvelope WriteInvocation(
        string channel,
        string methodName,
        object?[] args,
        IReadOnlyList<string>? excludedConnectionIds = null,
        string? invocationId = null,
        string? returnChannel = null,
        string? serverId = null)
    {
        var message = new SerializedHubMessage(new InvocationMessage(invocationId, methodName, args));
        var invocation = new MongoInvocation(message, excludedConnectionIds, invocationId, returnChannel);

        return new MongoBackplaneEnvelope(
            MongoBackplaneMessageKind.Invocation,
            channel,
            invocation,
            serverId,
            DateTimeOffset.UtcNow);
    }

    public MongoInvocation ReadInvocation(MongoBackplaneEnvelope envelope)
    {
        if (envelope.Kind != MongoBackplaneMessageKind.Invocation || envelope.Payload is not MongoInvocation invocation)
        {
            throw new InvalidDataException("Envelope does not contain an invocation payload.");
        }

        return invocation;
    }

    public static MongoBackplaneEnvelope WriteGroupCommand(string channel, MongoGroupCommand command, string? serverId = null)
    {
        return new MongoBackplaneEnvelope(
            MongoBackplaneMessageKind.GroupCommand,
            channel,
            command,
            serverId,
            DateTimeOffset.UtcNow);
    }

    public static MongoGroupCommand ReadGroupCommand(MongoBackplaneEnvelope envelope)
    {
        if (envelope.Kind != MongoBackplaneMessageKind.GroupCommand || envelope.Payload is not MongoGroupCommand command)
        {
            throw new InvalidDataException("Envelope does not contain a group command payload.");
        }

        return command;
    }

    public static MongoBackplaneEnvelope WriteAck(string channel, int id, string? serverId = null)
    {
        return new MongoBackplaneEnvelope(
            MongoBackplaneMessageKind.Ack,
            channel,
            id,
            serverId,
            DateTimeOffset.UtcNow);
    }

    public static int ReadAck(MongoBackplaneEnvelope envelope)
    {
        if (envelope.Kind != MongoBackplaneMessageKind.Ack || envelope.Payload is not int id)
        {
            throw new InvalidDataException("Envelope does not contain an acknowledgement payload.");
        }

        return id;
    }

    public static MongoBackplaneEnvelope WriteCompletion(
        string channel,
        string protocolName,
        ReadOnlyMemory<byte> completionMessage,
        string? serverId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(protocolName);

        return new MongoBackplaneEnvelope(
            MongoBackplaneMessageKind.Completion,
            channel,
            new MongoCompletion(protocolName, completionMessage.ToArray()),
            serverId,
            DateTimeOffset.UtcNow);
    }

    public static MongoCompletion ReadCompletion(MongoBackplaneEnvelope envelope)
    {
        if (envelope.Kind != MongoBackplaneMessageKind.Completion || envelope.Payload is not MongoCompletion completion)
        {
            throw new InvalidDataException("Envelope does not contain a completion payload.");
        }

        return completion;
    }
}
