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

        Func<MongoBackplaneEnvelope, CancellationToken, ValueTask>? singleHandler = null;
        Subscription[]? multiSnapshot = null;

        lock (subscriptions)
        {
            switch (subscriptions.Count)
            {
                case 0:
                    return 0;
                case 1:
                    // Capture the handler reference to avoid a Subscription[] allocation for
                    // the common case where a channel has exactly one subscriber (per-connection,
                    // per-user, and per-group channels almost always have a single subscriber).
                    singleHandler = subscriptions[0].Handler;
                    break;
                default:
                    multiSnapshot = subscriptions.ToArray();
                    break;
            }
        }

        if (singleHandler != null)
        {
            await singleHandler(envelope, cancellationToken);
            return 1;
        }

        var tasks = new Task[multiSnapshot!.Length];
        for (var i = 0; i < multiSnapshot.Length; i++)
        {
            tasks[i] = multiSnapshot[i].Handler(envelope, cancellationToken).AsTask();
        }

        await Task.WhenAll(tasks);
        return multiSnapshot.Length;
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

                    // Remove the channel entry when it is empty so that high-churn channels
                    // (per-connection, per-user, per-group) do not grow the dictionary without
                    // bound. Use the value-specific overload to atomically verify we are
                    // removing our own list, not a new one added by a concurrent Add() call.
                    if (subscriptions.Count == 0)
                    {
                        owner._subscriptions.TryRemove(
                            new KeyValuePair<string, List<Subscription>>(channel, subscriptions));
                    }
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
