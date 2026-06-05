using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR.Protocol;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoBackplaneDocumentSerializerTests
{
    [Fact]
    public void SerializeAddsStreamIdWithoutChangingEnvelope()
    {
        var serializer = CreateSerializer();
        var envelope = new MongoDbBackplaneProtocol().WriteInvocation("channel", "Hello", ["World"]);

        var document = serializer.Serialize("stream", envelope);
        var deserialized = serializer.Deserialize(document);

        Assert.Equal("stream", document["streamId"].AsString);
        Assert.Equal(envelope.Payload.GetType(), deserialized.Payload.GetType());
        Assert.Equal(envelope.Channel, deserialized.Channel);
    }

    private static MongoBackplaneDocumentSerializer CreateSerializer()
    {
        return new MongoBackplaneDocumentSerializer(
            new BsonBackplaneEnvelopeSerializer(new TestHubProtocolResolver(new JsonHubProtocol())));
    }
}
