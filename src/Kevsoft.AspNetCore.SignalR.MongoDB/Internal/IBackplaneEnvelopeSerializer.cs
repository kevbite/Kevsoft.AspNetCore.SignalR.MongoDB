namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IBackplaneEnvelopeSerializer
{
    byte[] Serialize(MongoBackplaneEnvelope envelope);

    MongoBackplaneEnvelope Deserialize(ReadOnlyMemory<byte> payload);
}
