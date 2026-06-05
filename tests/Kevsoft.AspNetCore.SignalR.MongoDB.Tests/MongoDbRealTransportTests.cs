using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoDbRealTransportTests
{
    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task TailableAwaitTransportRunsScaleoutBehaviorAgainstRealMongoDb()
    {
        await using var fixture = await MongoDbContainerFixture.StartAsync(replicaSet: false);
        var options = CreateOptions(MongoDbSignalRTransportMode.TailableAwait);

        await RunScaleoutBehaviorAsync(
            () => new MongoDbTailableAwaitBackplane(
                fixture.Database,
                options,
                CreateEnvelopeSerializer(),
                NullLogger<MongoDbTailableAwaitBackplane>.Instance),
            options);
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task ChangeStreamTransportRunsScaleoutBehaviorAgainstRealMongoDb()
    {
        await using var fixture = await MongoDbContainerFixture.StartAsync(replicaSet: true);
        var options = CreateOptions(MongoDbSignalRTransportMode.ChangeStreams);

        await RunScaleoutBehaviorAsync(
            () => new MongoDbChangeStreamBackplane(
                fixture.Database,
                options,
                CreateEnvelopeSerializer(),
                NullLogger<MongoDbChangeStreamBackplane>.Instance),
            options);
    }

    private static async Task RunScaleoutBehaviorAsync(
        Func<IMongoSignalRBackplane> createBackplane,
        MongoDbSignalROptions options)
    {
        await using var manager1 = CreateManager(createBackplane(), options);
        await using var manager2 = CreateManager(createBackplane(), options);

        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var connection1 = client1.CreateHubConnectionContext();
        var connection2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(connection1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(connection2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager1.SendAllAsync("Hello", ["World"]).WaitAsync(TimeSpan.FromSeconds(15));
        await AssertInvocationAsync(client1, "Hello", "World");
        await AssertInvocationAsync(client2, "Hello", "World");

        await manager2.SendConnectionAsync(connection1.ConnectionId, "Connection", ["Target"])
            .WaitAsync(TimeSpan.FromSeconds(15));
        await AssertInvocationAsync(client1, "Connection", "Target");

        await manager2.AddToGroupAsync(connection1.ConnectionId, "group").WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.SendGroupAsync("group", "Group", ["Member"]).WaitAsync(TimeSpan.FromSeconds(15));
        await AssertInvocationAsync(client1, "Group", "Member");

        var resultTask = manager2.InvokeConnectionAsync<int>(
            connection1.ConnectionId,
            "Result",
            ["value"],
            CancellationToken.None);
        var invocation = Assert.IsType<InvocationMessage>(
            await client1.ReadAsync().WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.NotNull(invocation.InvocationId);
        Assert.Equal("Result", invocation.Target);

        await manager1.SetConnectionResultAsync(
            connection1.ConnectionId,
            CompletionMessage.WithResult(invocation.InvocationId, 10));

        Assert.Equal(10L, await resultTask.WaitAsync(TimeSpan.FromSeconds(15)));
    }

    private static MongoDbSignalROptions CreateOptions(MongoDbSignalRTransportMode transportMode)
    {
        var options = new MongoDbSignalROptions
        {
            CollectionName = "messages_" + Guid.NewGuid().ToString("N"),
            ChannelPrefix = "test-hub",
            AckTimeout = TimeSpan.FromSeconds(10),
            ConnectionPresenceTtl = TimeSpan.FromMinutes(1),
        };

        if (transportMode == MongoDbSignalRTransportMode.TailableAwait)
            options.UseTailableAwait(o =>
            {
                o.MaxAwaitTime = TimeSpan.FromMilliseconds(100);
                o.CollectionSizeBytes = 1024 * 1024;
            });
        else
            options.UseChangeStreams(o => o.MessageTtl = TimeSpan.FromMinutes(5));

        return options;
    }

    private static MongoDbHubLifetimeManager<Hub> CreateManager(
        IMongoSignalRBackplane backplane,
        MongoDbSignalROptions options)
    {
        return new MongoDbHubLifetimeManager<Hub>(
            NullLogger<MongoDbHubLifetimeManager<Hub>>.Instance,
            Options.Create(options),
            CreateHubProtocolResolver(),
            backplane);
    }

    private static BsonBackplaneEnvelopeSerializer CreateEnvelopeSerializer()
    {
        return new BsonBackplaneEnvelopeSerializer(CreateHubProtocolResolver());
    }

    private static TestHubProtocolResolver CreateHubProtocolResolver()
    {
        return new TestHubProtocolResolver(new JsonHubProtocol());
    }

    private static async Task AssertInvocationAsync(MongoTestClient client, string target, string argument)
    {
        var message = Assert.IsType<InvocationMessage>(
            await client.ReadAsync().WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.Equal(target, message.Target);
        Assert.Equal(argument, Assert.Single(message.Arguments));
    }
}
