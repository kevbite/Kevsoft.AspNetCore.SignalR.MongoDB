using Kevsoft.AspNetCore.SignalR.MongoDB;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoDbHubLifetimeManagerTests
{
    [Fact]
    public async Task SendAllStartsBackplaneAndPublishesInvocationEnvelope()
    {
        var backplane = new FakeMongoSignalRBackplane();
        await using var manager = CreateManager(backplane);

        await manager.SendAllAsync("Hello", ["World"]);

        Assert.True(backplane.Started);
        var envelope = Assert.Single(backplane.Published);
        Assert.IsType<MongoInvocationPayload>(envelope.Payload);
    }

    [Fact]
    public async Task DisposeDisposesBackplane()
    {
        var backplane = new FakeMongoSignalRBackplane();
        var manager = CreateManager(backplane);

        await manager.DisposeAsync();

        Assert.True(backplane.Disposed);
    }

    [Fact]
    public async Task DisposeIsIdempotent()
    {
        var backplane = new FakeMongoSignalRBackplane();
        var manager = CreateManager(backplane);

        await manager.DisposeAsync();
        // Second dispose must not throw or double-dispose the backplane.
        var ex = await Record.ExceptionAsync(() => manager.DisposeAsync().AsTask());

        Assert.Null(ex);
    }

    [Fact]
    public async Task DisposeAfterStartupCleansUpCoreSubscriptions()
    {
        var backplane = new FakeMongoSignalRBackplane();
        await using var manager = CreateManager(backplane);

        // Trigger startup to register core subscriptions.
        await manager.SendAllAsync("Init", []);

        await manager.DisposeAsync();

        Assert.True(backplane.Disposed);
    }

    [Fact]
    public async Task DisposeAfterConnectedCleansUpConnectionSubscription()
    {
        var backplane = new FakeMongoSignalRBackplane();
        var manager = CreateManager(backplane);
        var client = CreateTestClient();
        var conn = client.CreateHubConnectionContext();

        await manager.OnConnectedAsync(conn);

        await manager.DisposeAsync();

        Assert.True(backplane.Disposed);
        // Backplane subscriptions for the connection channel should be removed.
        Assert.Empty(backplane.SubscriptionChannels);
    }

    [Fact]
    public async Task OnConnectedAfterDisposeThrowsObjectDisposedException()
    {
        var backplane = new FakeMongoSignalRBackplane();
        var manager = CreateManager(backplane);
        await manager.DisposeAsync();

        var client = CreateTestClient();
        var conn = client.CreateHubConnectionContext();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => manager.OnConnectedAsync(conn));
    }

    private static MongoDbHubLifetimeManager<Hub> CreateManager(FakeMongoSignalRBackplane backplane)
    {
        return new MongoDbHubLifetimeManager<Hub>(
            NullLogger<MongoDbHubLifetimeManager<Hub>>.Instance,
            Options.Create(new MongoDbSignalROptions { AckTimeout = TimeSpan.FromSeconds(1) }),
            new TestHubProtocolResolver(new JsonHubProtocol()),
            backplane);
    }

    private static TestClient CreateTestClient() => new();

    private sealed class TestClient : IDisposable
    {
        private readonly System.IO.Pipelines.Pipe _pipe = new();

        public Microsoft.AspNetCore.Connections.DefaultConnectionContext Connection { get; } =
            new(Guid.NewGuid().ToString("N"));

        public Microsoft.AspNetCore.SignalR.HubConnectionContext CreateHubConnectionContext(string? userId = null)
        {
            if (userId is not null)
            {
                Connection.User = new System.Security.Claims.ClaimsPrincipal(
                    new System.Security.Claims.ClaimsIdentity(
                        [new System.Security.Claims.Claim(System.Security.Claims.ClaimTypes.NameIdentifier, userId)]));
            }

            var ctx = new HubConnectionContext(
                Connection,
                new HubConnectionContextOptions { KeepAliveInterval = TimeSpan.FromSeconds(15) },
                NullLoggerFactory.Instance)
            {
                Protocol = new JsonHubProtocol()
            };

            if (userId is not null)
            {
                typeof(HubConnectionContext)
                    .GetProperty("UserIdentifier")!
                    .SetValue(ctx, userId);
            }

            return ctx;
        }

        public void Dispose() => _pipe.Reader.Complete();
    }
}
