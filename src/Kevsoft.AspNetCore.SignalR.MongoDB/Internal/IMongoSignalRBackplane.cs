namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IMongoSignalRBackplane : IMongoMessagePublisher, IMongoMessageSubscriber, IAsyncDisposable
{
    ValueTask StartAsync(CancellationToken cancellationToken = default);
}
