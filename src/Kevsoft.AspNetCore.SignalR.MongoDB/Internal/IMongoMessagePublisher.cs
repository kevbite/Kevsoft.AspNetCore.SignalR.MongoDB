namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IMongoMessagePublisher
{
    ValueTask PublishAsync(MongoBackplaneEnvelope envelope, CancellationToken cancellationToken = default);
}
