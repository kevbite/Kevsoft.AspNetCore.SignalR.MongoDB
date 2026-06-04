namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IMongoMessageSubscriber
{
    ValueTask<IAsyncDisposable> SubscribeAsync(
        string channel,
        Func<MongoBackplaneEnvelope, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default);
}
