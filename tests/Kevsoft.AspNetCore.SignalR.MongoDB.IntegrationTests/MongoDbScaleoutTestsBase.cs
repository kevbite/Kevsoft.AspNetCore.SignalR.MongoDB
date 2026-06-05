using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.IntegrationTests;

/// <summary>
/// Base class for scaleout lifecycle tests. Each concrete derived class provides
/// a specific MongoDB transport (TailableAwait or ChangeStreams) and a shared
/// container fixture. Test methods are inherited and discovered per transport.
/// </summary>
public abstract class MongoDbScaleoutTestsBase
{
    /// <summary>
    /// Creates a new <see cref="MongoDbHubLifetimeManager{THub}"/> backed by the real MongoDB
    /// transport, using the given collection name so that two managers share the same backplane.
    /// </summary>
    protected abstract MongoDbHubLifetimeManager<Hub> CreateManager(string collectionName);

    protected static string NewCollectionName() => "messages_" + Guid.NewGuid().ToString("N");

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task BroadcastReachesAllServers()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager1.SendAllAsync("Hello", ["World"]).WaitAsync(TimeSpan.FromSeconds(15));

        await AssertInvocationAsync(client1, "Hello", "World");
        await AssertInvocationAsync(client2, "Hello", "World");
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task BroadcastDoesNotReachDisconnectedConnections()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnDisconnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager1.SendAllAsync("Hello", ["World"]).WaitAsync(TimeSpan.FromSeconds(15));

        await AssertInvocationAsync(client1, "Hello", "World");
        Assert.False(client2.HasPendingMessage());
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task SendToRemoteConnectionDeliversMessage()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager2.SendConnectionAsync(conn1.ConnectionId, "Direct", ["Value"])
            .WaitAsync(TimeSpan.FromSeconds(15));

        await AssertInvocationAsync(client1, "Direct", "Value");
        Assert.False(client2.HasPendingMessage());
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task LocalConnectionSendDoesNotPublishToBackplane()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager1.SendConnectionAsync(conn1.ConnectionId, "Local", ["Only"])
            .WaitAsync(TimeSpan.FromSeconds(15));

        await AssertInvocationAsync(client1, "Local", "Only");
        Assert.False(client2.HasPendingMessage());
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task GroupMessagingFlowsAcrossServers()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager2.AddToGroupAsync(conn1.ConnectionId, "grp").WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.SendGroupAsync("grp", "Group", ["Msg"]).WaitAsync(TimeSpan.FromSeconds(15));

        await AssertInvocationAsync(client1, "Group", "Msg");
        Assert.False(client2.HasPendingMessage());
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task RemoveFromGroupStopsGroupMessages()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager2.AddToGroupAsync(conn1.ConnectionId, "grp").WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.RemoveFromGroupAsync(conn1.ConnectionId, "grp").WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.SendGroupAsync("grp", "Group", ["Msg"]).WaitAsync(TimeSpan.FromSeconds(15));

        await Task.Delay(500);
        Assert.False(client1.HasPendingMessage());
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task DisconnectRemovesConnectionFromGroup()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));
        await manager1.AddToGroupAsync(conn1.ConnectionId, "grp").WaitAsync(TimeSpan.FromSeconds(15));
        await manager1.OnDisconnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));

        await manager2.SendGroupAsync("grp", "Group", ["Msg"]).WaitAsync(TimeSpan.FromSeconds(15));

        await Task.Delay(500);
        Assert.False(client1.HasPendingMessage());
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task SendToUserReachesAllUserConnections()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext("user-42");
        var conn2 = client2.CreateHubConnectionContext("user-42");

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager1.SendUserAsync("user-42", "UserMsg", ["Data"]).WaitAsync(TimeSpan.FromSeconds(15));

        await AssertInvocationAsync(client1, "UserMsg", "Data");
        await AssertInvocationAsync(client2, "UserMsg", "Data");
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task ClientReturnResultFlowsAcrossServers()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        var resultTask = manager2.InvokeConnectionAsync<int>(
            conn1.ConnectionId,
            "Result",
            ["value"],
            CancellationToken.None);

        var invocation = Assert.IsType<InvocationMessage>(
            await client1.ReadAsync().WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.NotNull(invocation.InvocationId);

        await manager1.SetConnectionResultAsync(
            conn1.ConnectionId,
            CompletionMessage.WithResult(invocation.InvocationId, 10));

        Assert.Equal(10L, await resultTask.WaitAsync(TimeSpan.FromSeconds(15)));
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task ClientReturnErrorFlowsAcrossServers()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        var resultTask = manager2.InvokeConnectionAsync<int>(
            conn1.ConnectionId,
            "Result",
            ["value"],
            CancellationToken.None);

        var invocation = Assert.IsType<InvocationMessage>(
            await client1.ReadAsync().WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.NotNull(invocation.InvocationId);

        await manager1.SetConnectionResultAsync(
            conn1.ConnectionId,
            CompletionMessage.WithError(invocation.InvocationId, "Something went wrong."));

        var ex = await Assert.ThrowsAsync<HubException>(() => resultTask.WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.Equal("Something went wrong.", ex.Message);
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task ConnectionNotFoundFailsClientResult()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        using var client1 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));

        await Assert.ThrowsAnyAsync<Exception>(() =>
            manager1.InvokeConnectionAsync<int>(
                "nonexistent-connection-id",
                "Result",
                ["value"],
                CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(15)));
    }

    [DockerFact]
    [Trait("Category", "Integration")]
    public async Task MultipleGroupsSendToCorrectSubscribers()
    {
        var col = NewCollectionName();
        await using var manager1 = CreateManager(col);
        await using var manager2 = CreateManager(col);
        using var client1 = new MongoTestClient();
        using var client2 = new MongoTestClient();
        var conn1 = client1.CreateHubConnectionContext();
        var conn2 = client2.CreateHubConnectionContext();

        await manager1.OnConnectedAsync(conn1).WaitAsync(TimeSpan.FromSeconds(15));
        await manager2.OnConnectedAsync(conn2).WaitAsync(TimeSpan.FromSeconds(15));

        await manager1.AddToGroupAsync(conn1.ConnectionId, "groupA").WaitAsync(TimeSpan.FromSeconds(15));
        await manager1.AddToGroupAsync(conn2.ConnectionId, "groupB").WaitAsync(TimeSpan.FromSeconds(15));

        await manager2.SendGroupAsync("groupA", "ForA", ["a"]).WaitAsync(TimeSpan.FromSeconds(15));

        await AssertInvocationAsync(client1, "ForA", "a");
        Assert.False(client2.HasPendingMessage());
    }

    private static async Task AssertInvocationAsync(MongoTestClient client, string target, params string[] args)
    {
        var message = Assert.IsType<InvocationMessage>(
            await client.ReadAsync().WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.Equal(target, message.Target);
        for (var i = 0; i < args.Length; i++)
        {
            Assert.Equal(args[i], message.Arguments[i]);
        }
    }

    internal static MongoDbHubLifetimeManager<Hub> CreateManagerFromBackplane(
        IMongoSignalRBackplane backplane,
        MongoDbSignalROptions options)
    {
        return new MongoDbHubLifetimeManager<Hub>(
            NullLogger<MongoDbHubLifetimeManager<Hub>>.Instance,
            Options.Create(options),
            new TestHubProtocolResolver(new JsonHubProtocol()),
            backplane);
    }

    internal static BsonBackplaneEnvelopeSerializer CreateEnvelopeSerializer()
    {
        return new BsonBackplaneEnvelopeSerializer(new TestHubProtocolResolver(new JsonHubProtocol()));
    }
}
