using System.Buffers;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR.Protocol;
using MongoDB.Bson;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class BsonBackplaneEnvelopeSerializerTests
{
    private readonly JsonHubProtocol _jsonProtocol = new();

    [Fact]
    public void InvocationRoundTripsThroughBson()
    {
        var protocol = new MongoDbBackplaneProtocol();
        var serializer = CreateSerializer();
        var envelope = protocol.WriteInvocation(
            "hub:all",
            "Hello",
            ["World"],
            excludedConnectionIds: ["connection-1"],
            invocationId: "invocation-1",
            returnChannel: "return-channel",
            serverId: "server-1");

        var deserialized = serializer.Deserialize(serializer.Serialize(envelope));
        var invocation = protocol.ReadInvocation(deserialized);

        Assert.IsType<MongoInvocationPayload>(deserialized.Payload);
        Assert.Equal("hub:all", deserialized.Channel);
        Assert.Equal("invocation-1", invocation.InvocationId);
        Assert.Equal("return-channel", invocation.ReturnChannel);
        Assert.Equal(["connection-1"], invocation.ExcludedConnectionIds);

        var sequence = new ReadOnlySequence<byte>(invocation.Message.GetSerializedMessage(_jsonProtocol).ToArray());
        var binder = new TestInvocationBinder { ParameterTypes = [typeof(string)] };
        Assert.True(_jsonProtocol.TryParseMessage(ref sequence, binder, out var hubMessage));
        var parsedInvocation = Assert.IsType<InvocationMessage>(hubMessage);
        Assert.Equal("Hello", parsedInvocation.Target);
        Assert.Equal("World", Assert.Single(parsedInvocation.Arguments));
    }

    [Fact]
    public void GroupCommandRoundTripsThroughBson()
    {
        var serializer = CreateSerializer();
        var command = new MongoGroupCommand(42, "server-a", GroupAction.Add, "group", "connection");
        var envelope = MongoDbBackplaneProtocol.WriteGroupCommand("groups", command, "server-b");

        var deserialized = serializer.Deserialize(serializer.Serialize(envelope));
        var actual = MongoDbBackplaneProtocol.ReadGroupCommand(deserialized);

        Assert.Equal(command, actual);
    }

    [Fact]
    public void AckRoundTripsThroughBson()
    {
        var serializer = CreateSerializer();
        var envelope = MongoDbBackplaneProtocol.WriteAck("ack:server-a", 42, "server-b");

        var deserialized = serializer.Deserialize(serializer.Serialize(envelope));

        Assert.Equal(42, MongoDbBackplaneProtocol.ReadAck(deserialized));
    }

    [Fact]
    public void CompletionRoundTripsThroughBson()
    {
        var serializer = CreateSerializer();
        var payload = new byte[] { 1, 2, 3 };
        var envelope = MongoDbBackplaneProtocol.WriteCompletion("return", "json", payload, "server");

        var deserialized = serializer.Deserialize(serializer.Serialize(envelope));
        var completion = MongoDbBackplaneProtocol.ReadCompletion(deserialized);

        Assert.Equal("json", completion.ProtocolName);
        Assert.Equal(payload, completion.CompletionMessage);
    }

    [Fact]
    public void DeserializeThrowsForUnsupportedVersion()
    {
        var serializer = CreateSerializer();
        var envelope = CreateSerializer().Serialize(
            new MongoDbBackplaneProtocol().WriteInvocation("ch", "M", []));
        envelope["version"] = 999;

        Assert.Throws<InvalidDataException>(() => serializer.Deserialize(envelope));
    }

    [Fact]
    public void DeserializeThrowsForUnknownPayloadDiscriminator()
    {
        var serializer = CreateSerializer();
        var envelope = CreateSerializer().Serialize(
            new MongoDbBackplaneProtocol().WriteInvocation("ch", "M", []));
        // Replace the _t discriminator inside the payload subdocument.
        envelope["payload"].AsBsonDocument["_t"] = "unknown_type";

        Assert.Throws<InvalidDataException>(() => serializer.Deserialize(envelope));
    }

    [Fact]
    public void DeserializeThrowsForMissingVersionField()
    {
        var serializer = CreateSerializer();
        var envelope = CreateSerializer().Serialize(
            new MongoDbBackplaneProtocol().WriteInvocation("ch", "M", []));
        envelope.Remove("version");

        // GetValue("version") throws when the field is absent.
        Assert.ThrowsAny<Exception>(() => serializer.Deserialize(envelope));
    }

    [Fact]
    public void DeserializeThrowsForMissingChannelField()
    {
        var serializer = CreateSerializer();
        var envelope = CreateSerializer().Serialize(
            new MongoDbBackplaneProtocol().WriteInvocation("ch", "M", []));
        envelope.Remove("channel");

        Assert.ThrowsAny<Exception>(() => serializer.Deserialize(envelope));
    }

    [Fact]
    public void DeserializeDocumentWithExtraFieldsSucceeds()
    {
        var serializer = CreateSerializer();
        var base_ = serializer.Serialize(
            new MongoDbBackplaneProtocol().WriteInvocation("ch", "M", []));
        base_["unknownFutureField"] = "value";

        // Forward-compatible: extra top-level fields must not cause a failure.
        var result = serializer.Deserialize(base_);

        Assert.Equal("ch", result.Channel);
    }

    private BsonBackplaneEnvelopeSerializer CreateSerializer()
    {
        return new BsonBackplaneEnvelopeSerializer(new TestHubProtocolResolver(_jsonProtocol));
    }
}
