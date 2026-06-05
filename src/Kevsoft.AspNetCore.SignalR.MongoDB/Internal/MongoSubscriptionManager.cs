using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoSubscriptionManager : IAsyncDisposable
{
    // Stripe count: enough buckets to minimise cross-channel collisions while
    // keeping memory overhead negligible. Independent channels that hash to
    // different stripes can now add/remove subscriptions concurrently instead
    // of serialising behind a single global lock.
    private static readonly int StripeCount = Math.Max(8, Environment.ProcessorCount * 2);

    private readonly ConcurrentDictionary<string, HubConnectionStore> _subscriptions = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IAsyncDisposable> _subscriptionDisposables = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim[] _stripes =
        Enumerable.Range(0, StripeCount).Select(_ => new SemaphoreSlim(1, 1)).ToArray();

    private SemaphoreSlim GetStripe(string id)
    {
        // Use unsigned modulo to avoid negative indices from negative hash codes.
        var index = (uint)string.GetHashCode(id, StringComparison.Ordinal) % (uint)StripeCount;
        return _stripes[index];
    }

    public async Task AddSubscriptionAsync(
        string id,
        HubConnectionContext connection,
        Func<string, HubConnectionStore, ValueTask<IAsyncDisposable>> subscribeMethod)
    {
        var stripe = GetStripe(id);
        await stripe.WaitAsync();

        try
        {
            if (connection.ConnectionAborted.IsCancellationRequested)
            {
                return;
            }

            var subscription = _subscriptions.GetOrAdd(id, _ => new HubConnectionStore());
            subscription.Add(connection);

            if (subscription.Count == 1)
            {
                var disposable = await subscribeMethod(id, subscription);
                _subscriptionDisposables[id] = disposable;
            }
        }
        finally
        {
            stripe.Release();
        }
    }

    public async Task RemoveSubscriptionAsync(string id, HubConnectionContext connection)
    {
        var stripe = GetStripe(id);
        await stripe.WaitAsync();

        try
        {
            if (!_subscriptions.TryGetValue(id, out var subscription))
            {
                return;
            }

            subscription.Remove(connection);

            if (subscription.Count == 0)
            {
                _subscriptions.TryRemove(id, out _);
                if (_subscriptionDisposables.TryRemove(id, out var disposable))
                {
                    await disposable.DisposeAsync();
                }
            }
        }
        finally
        {
            stripe.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var disposable in _subscriptionDisposables.Values)
        {
            await disposable.DisposeAsync();
        }

        _subscriptionDisposables.Clear();
        _subscriptions.Clear();

        foreach (var stripe in _stripes)
        {
            stripe.Dispose();
        }
    }
}
