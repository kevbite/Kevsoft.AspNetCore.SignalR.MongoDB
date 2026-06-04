using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using MongoDB.Bson;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class BsonBackplaneEnvelopeSerializer : IBackplaneEnvelopeSerializer
{
    private const int CurrentVersion = 1;
    private readonly IReadOnlyList<IHubProtocol> _hubProtocols;

    public BsonBackplaneEnvelopeSerializer(IHubProtocolResolver hubProtocolResolver)
    {
        _hubProtocols = hubProtocolResolver.AllProtocols.ToArray();
    }

    public BsonDocument Serialize(MongoBackplaneEnvelope envelope)
    {
        var document = new BsonDocument
        {
            ["version"] = CurrentVersion,
            ["kind"] = envelope.Kind.ToString(),
            ["channel"] = envelope.Channel,
            ["payload"] = SerializePayload(envelope),
            ["createdAtUtc"] = new BsonDateTime((envelope.CreatedAt ?? DateTimeOffset.UtcNow).UtcDateTime)
        };

        if (!string.IsNullOrEmpty(envelope.ServerId))
        {
            document["serverId"] = envelope.ServerId;
        }

        return document;
    }

    public MongoBackplaneEnvelope Deserialize(BsonDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);

        var version = document.GetValue("version").ToInt32();
        if (version != CurrentVersion)
        {
            throw new InvalidDataException($"Unsupported MongoDB SignalR backplane envelope version '{version}'.");
        }

        var kind = Enum.Parse<MongoBackplaneMessageKind>(document.GetValue("kind").AsString);
        var channel = document.GetValue("channel").AsString;
        var payload = DeserializePayload(kind, document.GetValue("payload").AsBsonDocument);
        var serverId = document.TryGetValue("serverId", out var serverIdValue) ? serverIdValue.AsString : null;
        var createdAt = document.TryGetValue("createdAtUtc", out var createdAtValue)
            ? new DateTimeOffset(createdAtValue.ToUniversalTime(), TimeSpan.Zero)
            : (DateTimeOffset?)null;

        return new MongoBackplaneEnvelope(kind, channel, payload, serverId, createdAt);
    }

    private BsonDocument SerializePayload(MongoBackplaneEnvelope envelope)
    {
        return envelope.Kind switch
        {
            MongoBackplaneMessageKind.Invocation => SerializeInvocation((MongoInvocation)envelope.Payload),
            MongoBackplaneMessageKind.GroupCommand => SerializeGroupCommand((MongoGroupCommand)envelope.Payload),
            MongoBackplaneMessageKind.Ack => new BsonDocument("id", (int)envelope.Payload),
            MongoBackplaneMessageKind.Completion => SerializeCompletion((MongoCompletion)envelope.Payload),
            _ => throw new InvalidDataException($"Unsupported backplane message kind '{envelope.Kind}'.")
        };
    }

    private object DeserializePayload(MongoBackplaneMessageKind kind, BsonDocument payload)
    {
        return kind switch
        {
            MongoBackplaneMessageKind.Invocation => DeserializeInvocation(payload),
            MongoBackplaneMessageKind.GroupCommand => DeserializeGroupCommand(payload),
            MongoBackplaneMessageKind.Ack => payload.GetValue("id").ToInt32(),
            MongoBackplaneMessageKind.Completion => DeserializeCompletion(payload),
            _ => throw new InvalidDataException($"Unsupported backplane message kind '{kind}'.")
        };
    }

    private BsonDocument SerializeInvocation(MongoInvocation invocation)
    {
        var messages = new BsonDocument();
        foreach (var protocol in _hubProtocols)
        {
            messages[protocol.Name] = new BsonBinaryData(invocation.Message.GetSerializedMessage(protocol).ToArray());
        }

        var document = new BsonDocument
        {
            ["messages"] = messages
        };

        if (invocation.ExcludedConnectionIds is { Count: > 0 })
        {
            document["excludedConnectionIds"] = new BsonArray(invocation.ExcludedConnectionIds);
        }

        if (!string.IsNullOrEmpty(invocation.InvocationId))
        {
            document["invocationId"] = invocation.InvocationId;
        }

        if (!string.IsNullOrEmpty(invocation.ReturnChannel))
        {
            document["returnChannel"] = invocation.ReturnChannel;
        }

        return document;
    }

    private static MongoInvocation DeserializeInvocation(BsonDocument payload)
    {
        var messagesDocument = payload.GetValue("messages").AsBsonDocument;
        var messages = new List<SerializedMessage>(messagesDocument.ElementCount);

        foreach (var element in messagesDocument)
        {
            messages.Add(new SerializedMessage(element.Name, element.Value.AsBsonBinaryData.Bytes));
        }

        IReadOnlyList<string>? excludedConnectionIds = null;
        if (payload.TryGetValue("excludedConnectionIds", out var excludedValue))
        {
            excludedConnectionIds = excludedValue.AsBsonArray.Select(value => value.AsString).ToArray();
        }

        var invocationId = payload.TryGetValue("invocationId", out var invocationValue) ? invocationValue.AsString : null;
        var returnChannel = payload.TryGetValue("returnChannel", out var returnValue) ? returnValue.AsString : null;

        return new MongoInvocation(new SerializedHubMessage(messages), excludedConnectionIds, invocationId, returnChannel);
    }

    private static BsonDocument SerializeGroupCommand(MongoGroupCommand command)
    {
        return new BsonDocument
        {
            ["id"] = command.Id,
            ["serverName"] = command.ServerName,
            ["action"] = (int)command.Action,
            ["groupName"] = command.GroupName,
            ["connectionId"] = command.ConnectionId
        };
    }

    private static MongoGroupCommand DeserializeGroupCommand(BsonDocument payload)
    {
        return new MongoGroupCommand(
            payload.GetValue("id").ToInt32(),
            payload.GetValue("serverName").AsString,
            (GroupAction)payload.GetValue("action").ToInt32(),
            payload.GetValue("groupName").AsString,
            payload.GetValue("connectionId").AsString);
    }

    private static BsonDocument SerializeCompletion(MongoCompletion completion)
    {
        return new BsonDocument
        {
            ["protocolName"] = completion.ProtocolName,
            ["message"] = new BsonBinaryData(completion.CompletionMessage)
        };
    }

    private static MongoCompletion DeserializeCompletion(BsonDocument payload)
    {
        return new MongoCompletion(
            payload.GetValue("protocolName").AsString,
            payload.GetValue("message").AsBsonBinaryData.Bytes);
    }
}
