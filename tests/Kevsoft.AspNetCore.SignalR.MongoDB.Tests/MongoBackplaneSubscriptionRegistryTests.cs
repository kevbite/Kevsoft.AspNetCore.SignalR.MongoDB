using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoBackplaneSubscriptionRegistryTests
{
    [Fact]
    public async Task DispatchDeliversToAllSubscribersOnChannel()
    {
        var registry = new MongoBackplaneSubscriptionRegistry();
        var received = new List<string>();

        await using var sub1 = registry.Add("ch", (env, _) =>
        {
            received.Add("sub1");
            return ValueTask.CompletedTask;
        });
        await using var sub2 = registry.Add("ch", (env, _) =>
        {
            received.Add("sub2");
            return ValueTask.CompletedTask;
        });

        var envelope = new MongoBackplaneEnvelope("ch", new MongoAckPayload(0), null, null);
        var count = await registry.DispatchAsync(envelope, CancellationToken.None);

        Assert.Equal(2, count);
        Assert.Contains("sub1", received);
        Assert.Contains("sub2", received);
    }

    [Fact]
    public async Task DispatchReturnsZeroWhenNoSubscribersExist()
    {
        var registry = new MongoBackplaneSubscriptionRegistry();
        var envelope = new MongoBackplaneEnvelope("unknown", new MongoAckPayload(0), null, null);

        var count = await registry.DispatchAsync(envelope, CancellationToken.None);

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task DisposingLastSubscriberRemovesChannelEntry()
    {
        var registry = new MongoBackplaneSubscriptionRegistry();
        var received = false;

        var sub = registry.Add("ch", (_, _) =>
        {
            received = true;
            return ValueTask.CompletedTask;
        });

        await sub.DisposeAsync();

        // After disposal, dispatching to the channel must not deliver to the removed subscriber.
        var envelope = new MongoBackplaneEnvelope("ch", new MongoAckPayload(0), null, null);
        var count = await registry.DispatchAsync(envelope, CancellationToken.None);

        Assert.Equal(0, count);
        Assert.False(received);
    }

    [Fact]
    public async Task DisposingOneOfTwoSubscribersKeepsChannelAlive()
    {
        var registry = new MongoBackplaneSubscriptionRegistry();
        var receivedBySecond = false;

        var sub1 = registry.Add("ch", (_, _) => ValueTask.CompletedTask);
        await using var sub2 = registry.Add("ch", (_, _) =>
        {
            receivedBySecond = true;
            return ValueTask.CompletedTask;
        });

        await sub1.DisposeAsync();

        var envelope = new MongoBackplaneEnvelope("ch", new MongoAckPayload(0), null, null);
        var count = await registry.DispatchAsync(envelope, CancellationToken.None);

        Assert.Equal(1, count);
        Assert.True(receivedBySecond);
    }

    [Fact]
    public async Task AddAfterDisposeOfLastSubscriberWorks()
    {
        var registry = new MongoBackplaneSubscriptionRegistry();

        var sub1 = registry.Add("ch", (_, _) => ValueTask.CompletedTask);
        await sub1.DisposeAsync();

        var received = false;
        await using var sub2 = registry.Add("ch", (_, _) =>
        {
            received = true;
            return ValueTask.CompletedTask;
        });

        var envelope = new MongoBackplaneEnvelope("ch", new MongoAckPayload(0), null, null);
        await registry.DispatchAsync(envelope, CancellationToken.None);

        Assert.True(received);
    }

    [Fact]
    public async Task ClearRemovesAllSubscriptions()
    {
        var registry = new MongoBackplaneSubscriptionRegistry();
        var received = false;

        registry.Add("ch", (_, _) =>
        {
            received = true;
            return ValueTask.CompletedTask;
        });

        registry.Clear();

        var envelope = new MongoBackplaneEnvelope("ch", new MongoAckPayload(0), null, null);
        await registry.DispatchAsync(envelope, CancellationToken.None);

        Assert.False(received);
    }
}
