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
            ["channel"] = envelope.Channel,
            ["payload"] = SerializePayload(envelope.Payload),
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

        var channel = document.GetValue("channel").AsString;
        var payload = DeserializePayload(document.GetValue("payload").AsBsonDocument);
        var serverId = document.TryGetValue("serverId", out var serverIdValue) ? serverIdValue.AsString : null;
        var createdAt = document.TryGetValue("createdAtUtc", out var createdAtValue)
            ? new DateTimeOffset(createdAtValue.ToUniversalTime(), TimeSpan.Zero)
            : (DateTimeOffset?)null;

        return new MongoBackplaneEnvelope(channel, payload, serverId, createdAt);
    }

    private BsonDocument SerializePayload(MongoBackplanePayload payload)
    {
        // The _t discriminator is written first so it matches the conventional MongoDB
        // document shape (discriminator before data fields).
        return payload switch
        {
            MongoInvocationPayload p  => SerializeInvocation(p.Invocation).Add("_t", MongoInvocationPayload.TypeDiscriminator),
            MongoGroupCommandPayload p => SerializeGroupCommand(p.Command).Add("_t", MongoGroupCommandPayload.TypeDiscriminator),
            MongoAckPayload p          => new BsonDocument { ["_t"] = MongoAckPayload.TypeDiscriminator, ["id"] = p.Id },
            MongoCompletionPayload p   => SerializeCompletion(p.Completion).Add("_t", MongoCompletionPayload.TypeDiscriminator),
            _ => throw new InvalidDataException($"Unsupported payload type '{payload.GetType().Name}'.")
        };
    }

    private static MongoBackplanePayload DeserializePayload(BsonDocument payload)
    {
        var discriminator = payload.GetValue("_t").AsString;
        return discriminator switch
        {
            MongoInvocationPayload.TypeDiscriminator  => new MongoInvocationPayload(DeserializeInvocation(payload)),
            MongoGroupCommandPayload.TypeDiscriminator => new MongoGroupCommandPayload(DeserializeGroupCommand(payload)),
            MongoAckPayload.TypeDiscriminator          => new MongoAckPayload(payload.GetValue("id").ToInt32()),
            MongoCompletionPayload.TypeDiscriminator   => new MongoCompletionPayload(DeserializeCompletion(payload)),
            _ => throw new InvalidDataException($"Unsupported backplane payload discriminator '{discriminator}'.")
        };
    }

    private BsonDocument SerializeInvocation(MongoInvocation invocation)
    {
        // Each registered hub protocol produces its own wire encoding of the invocation.
        // All protocols are serialised eagerly here so that every server in the cluster
        // can dispatch the message to its own connections regardless of which protocol
        // those connections negotiate. This is the same "serialise once, dispatch cheaply"
        // trade-off used by the ASP.NET Core Redis backplane.
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
