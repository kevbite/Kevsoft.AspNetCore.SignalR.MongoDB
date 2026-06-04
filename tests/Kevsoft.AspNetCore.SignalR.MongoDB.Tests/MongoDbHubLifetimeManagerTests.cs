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
        Assert.Equal(MongoBackplaneMessageKind.Invocation, envelope.Kind);
    }

    [Fact]
    public async Task DisposeDisposesBackplane()
    {
        var backplane = new FakeMongoSignalRBackplane();
        var manager = CreateManager(backplane);

        await manager.DisposeAsync();

        Assert.True(backplane.Disposed);
    }

    private static MongoDbHubLifetimeManager<Hub> CreateManager(FakeMongoSignalRBackplane backplane)
    {
        return new MongoDbHubLifetimeManager<Hub>(
            NullLogger<MongoDbHubLifetimeManager<Hub>>.Instance,
            Options.Create(new MongoDbSignalROptions { AckTimeout = TimeSpan.FromSeconds(1) }),
            new TestHubProtocolResolver(new JsonHubProtocol()),
            backplane);
    }
}
