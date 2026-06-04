using System.Collections.Concurrent;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoBackplaneSubscriptionRegistry
{
    private readonly ConcurrentDictionary<string, List<Subscription>> _subscriptions = new(StringComparer.Ordinal);

    public IAsyncDisposable Add(string channel, Func<MongoBackplaneEnvelope, CancellationToken, ValueTask> handler)
    {
        var subscription = new Subscription(channel, handler, this);
        var subscriptions = _subscriptions.GetOrAdd(channel, _ => []);

        lock (subscriptions)
        {
            subscriptions.Add(subscription);
        }

        return subscription;
    }

    public async ValueTask<int> DispatchAsync(MongoBackplaneEnvelope envelope, CancellationToken cancellationToken)
    {
        if (!_subscriptions.TryGetValue(envelope.Channel, out var subscriptions))
        {
            return 0;
        }

        Subscription[] snapshot;
        lock (subscriptions)
        {
            snapshot = subscriptions.ToArray();
        }

        var tasks = new Task[snapshot.Length];
        for (var i = 0; i < snapshot.Length; i++)
        {
            tasks[i] = snapshot[i].Handler(envelope, cancellationToken).AsTask();
        }

        await Task.WhenAll(tasks);
        return snapshot.Length;
    }

    public void Clear()
    {
        _subscriptions.Clear();
    }

    private sealed class Subscription(
        string channel,
        Func<MongoBackplaneEnvelope, CancellationToken, ValueTask> handler,
        MongoBackplaneSubscriptionRegistry owner) : IAsyncDisposable
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
