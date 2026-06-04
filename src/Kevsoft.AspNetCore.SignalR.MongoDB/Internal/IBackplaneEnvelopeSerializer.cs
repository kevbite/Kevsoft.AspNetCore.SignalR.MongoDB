using MongoDB.Bson;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IBackplaneEnvelopeSerializer
{
    BsonDocument Serialize(MongoBackplaneEnvelope envelope);

    MongoBackplaneEnvelope Deserialize(BsonDocument document);
}
