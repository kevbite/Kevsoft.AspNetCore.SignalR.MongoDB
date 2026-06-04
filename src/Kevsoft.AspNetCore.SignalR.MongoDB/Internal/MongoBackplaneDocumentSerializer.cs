using MongoDB.Bson;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoBackplaneDocumentSerializer(IBackplaneEnvelopeSerializer envelopeSerializer)
{
    public BsonDocument Serialize(string streamId, MongoBackplaneEnvelope envelope)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var document = envelopeSerializer.Serialize(envelope);
        document[MongoBackplaneDocumentFields.StreamId] = streamId;
        return document;
    }

    public MongoBackplaneEnvelope Deserialize(BsonDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);
        return envelopeSerializer.Deserialize(document);
    }
}
