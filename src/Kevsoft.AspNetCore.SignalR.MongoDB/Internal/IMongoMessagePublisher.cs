namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IMongoMessagePublisher
{
    ValueTask<long> PublishAsync(MongoBackplaneEnvelope envelope, CancellationToken cancellationToken = default);
}
