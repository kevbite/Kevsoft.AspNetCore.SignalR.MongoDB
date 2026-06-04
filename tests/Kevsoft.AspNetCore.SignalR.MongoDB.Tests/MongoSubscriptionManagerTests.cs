using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoSubscriptionManagerTests
{
    [Fact]
    public async Task AddSubscriptionSubscribesOncePerChannel()
    {
        await using var manager = new MongoSubscriptionManager();
        var connection1 = CreateConnectionContext("connection-1");
        var connection2 = CreateConnectionContext("connection-2");
        var subscribeCount = 0;

        await manager.AddSubscriptionAsync("channel", connection1, (_, _) =>
        {
            subscribeCount++;
            return ValueTask.FromResult<IAsyncDisposable>(new NoopAsyncDisposable());
        });
        await manager.AddSubscriptionAsync("channel", connection2, (_, _) =>
        {
            subscribeCount++;
            return ValueTask.FromResult<IAsyncDisposable>(new NoopAsyncDisposable());
        });

        Assert.Equal(1, subscribeCount);
    }

    [Fact]
    public async Task RemoveSubscriptionDisposesWhenLastConnectionLeaves()
    {
        await using var manager = new MongoSubscriptionManager();
        var connection = CreateConnectionContext("connection");
        var disposable = new TrackingAsyncDisposable();

        await manager.AddSubscriptionAsync("channel", connection, (_, _) => ValueTask.FromResult<IAsyncDisposable>(disposable));
        await manager.RemoveSubscriptionAsync("channel", connection);

        Assert.True(disposable.Disposed);
    }

    private static HubConnectionContext CreateConnectionContext(string connectionId)
    {
        var connectionContext = new DefaultConnectionContext(connectionId);
        var options = new HubConnectionContextOptions
        {
            KeepAliveInterval = TimeSpan.FromSeconds(15),
            ClientTimeoutInterval = TimeSpan.FromSeconds(15)
        };

        return new HubConnectionContext(connectionContext, options, NullLoggerFactory.Instance)
        {
            Protocol = new JsonHubProtocol()
        };
    }

    private sealed class NoopAsyncDisposable : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TrackingAsyncDisposable : IAsyncDisposable
    {
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
