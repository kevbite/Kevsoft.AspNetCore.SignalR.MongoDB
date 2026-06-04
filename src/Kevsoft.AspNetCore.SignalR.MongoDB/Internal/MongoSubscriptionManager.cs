using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoSubscriptionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, HubConnectionStore> _subscriptions = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IAsyncDisposable> _subscriptionDisposables = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _lock = new(1, 1);

    public async Task AddSubscriptionAsync(
        string id,
        HubConnectionContext connection,
        Func<string, HubConnectionStore, ValueTask<IAsyncDisposable>> subscribeMethod)
    {
        await _lock.WaitAsync();

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
            _lock.Release();
        }
    }

    public async Task RemoveSubscriptionAsync(string id, HubConnectionContext connection)
    {
        await _lock.WaitAsync();

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
            _lock.Release();
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
        _lock.Dispose();
    }
}
