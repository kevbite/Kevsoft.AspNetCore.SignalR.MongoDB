namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal interface IMongoSignalRBackplane : IMongoMessagePublisher, IMongoMessageSubscriber, IAsyncDisposable
{
    ValueTask StartAsync(string streamId, CancellationToken cancellationToken = default);

    ValueTask AddConnectionAsync(string connectionId, string serverId, CancellationToken cancellationToken = default);

    ValueTask RemoveConnectionAsync(string connectionId, string serverId, CancellationToken cancellationToken = default);

    ValueTask<bool> HasConnectionAsync(string connectionId, CancellationToken cancellationToken = default);
}
