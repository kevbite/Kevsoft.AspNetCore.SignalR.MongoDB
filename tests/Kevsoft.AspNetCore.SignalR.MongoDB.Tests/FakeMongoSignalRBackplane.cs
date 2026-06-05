using System.Collections.Concurrent;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

internal sealed class FakeMongoSignalRBackplane : IMongoSignalRBackplane
{
    private readonly ConcurrentDictionary<string, List<Subscription>> _subscriptions = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, string> _connections = new(StringComparer.Ordinal);

    public bool Started { get; private set; }

    public bool Disposed { get; private set; }

    public List<MongoBackplaneEnvelope> Published { get; } = [];

    /// <summary>Returns the set of channels that currently have at least one subscriber.</summary>
    public IReadOnlyCollection<string> SubscriptionChannels => _subscriptions
        .Where(kv => { lock (kv.Value) { return kv.Value.Count > 0; } })
        .Select(kv => kv.Key)
        .ToArray();

    public ValueTask StartAsync(string streamId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        cancellationToken.ThrowIfCancellationRequested();
        Started = true;
        return ValueTask.CompletedTask;
    }

    public async ValueTask<long> PublishAsync(MongoBackplaneEnvelope envelope, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Published.Add(envelope);

        if (!_subscriptions.TryGetValue(envelope.Channel, out var subscriptions))
        {
            return 0;
        }

        var snapshot = subscriptions.ToArray();
        foreach (var subscription in snapshot)
        {
            await subscription.Handler(envelope, cancellationToken);
        }

        return snapshot.Length;
    }

    public ValueTask<IAsyncDisposable> SubscribeAsync(
        string channel,
        Func<MongoBackplaneEnvelope, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var subscription = new Subscription(channel, handler, this);
        var subscriptions = _subscriptions.GetOrAdd(channel, _ => []);
        lock (subscriptions)
        {
            subscriptions.Add(subscription);
        }

        return ValueTask.FromResult<IAsyncDisposable>(subscription);
    }

    public ValueTask AddConnectionAsync(string connectionId, string serverId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _connections[connectionId] = serverId;
        return ValueTask.CompletedTask;
    }

    public ValueTask RemoveConnectionAsync(string connectionId, string serverId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _connections.TryRemove(new KeyValuePair<string, string>(connectionId, serverId));
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> HasConnectionAsync(string connectionId, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_connections.ContainsKey(connectionId));
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        _subscriptions.Clear();
        _connections.Clear();
        return ValueTask.CompletedTask;
    }

    private sealed class Subscription(
        string channel,
        Func<MongoBackplaneEnvelope, CancellationToken, ValueTask> handler,
        FakeMongoSignalRBackplane owner) : IAsyncDisposable
    {
        public Func<MongoBackplaneEnvelope, CancellationToken, ValueTask> Handler { get; } = handler;

        public ValueTask DisposeAsync()
        {
            if (owner._subscriptions.TryGetValue(channel, out var subscriptions))
            {
                lock (subscriptions)
                {
                    subscriptions.Remove(this);
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
