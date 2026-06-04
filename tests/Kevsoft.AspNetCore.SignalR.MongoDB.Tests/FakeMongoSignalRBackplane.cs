using System.Collections.Concurrent;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

internal sealed class FakeMongoSignalRBackplane : IMongoSignalRBackplane
{
    private readonly ConcurrentDictionary<string, List<Subscription>> _subscriptions = new(StringComparer.Ordinal);

    public bool Started { get; private set; }

    public bool Disposed { get; private set; }

    public List<MongoBackplaneEnvelope> Published { get; } = [];

    public ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
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

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        _subscriptions.Clear();
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
