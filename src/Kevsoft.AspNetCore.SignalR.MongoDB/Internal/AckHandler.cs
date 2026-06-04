using System.Collections.Concurrent;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class AckHandler : IDisposable
{
    private readonly ConcurrentDictionary<int, AckInfo> _acks = new();
    private readonly TimeSpan _ackTimeout;
    private readonly Timer _timer;
    private readonly object _lock = new();
    private bool _disposed;

    public AckHandler(TimeSpan ackTimeout)
    {
        if (ackTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ackTimeout), "Ack timeout must be positive.");
        }

        _ackTimeout = ackTimeout;
        var interval = TimeSpan.FromMilliseconds(Math.Min(Math.Max(ackTimeout.TotalMilliseconds / 6, 100), 5000));
        _timer = new Timer(static state => ((AckHandler)state!).CheckAcks(), this, interval, interval);
    }

    public Task CreateAck(int id)
    {
        lock (_lock)
        {
            if (_disposed)
            {
                return Task.CompletedTask;
            }

            return _acks.GetOrAdd(id, _ => new AckInfo()).TaskCompletionSource.Task;
        }
    }

    public void TriggerAck(int id)
    {
        if (_acks.TryRemove(id, out var ack))
        {
            ack.TaskCompletionSource.TrySetResult();
        }
    }

    private void CheckAcks()
    {
        if (_disposed)
        {
            return;
        }

        var currentTick = Environment.TickCount64;
        foreach (var pair in _acks)
        {
            if (TimeSpan.FromMilliseconds(currentTick - pair.Value.CreatedTick) > _ackTimeout &&
                _acks.TryRemove(pair.Key, out var ack))
            {
                ack.TaskCompletionSource.TrySetCanceled();
            }
        }
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;
            _timer.Dispose();

            foreach (var pair in _acks)
            {
                if (_acks.TryRemove(pair.Key, out var ack))
                {
                    ack.TaskCompletionSource.TrySetCanceled();
                }
            }
        }
    }

    private sealed class AckInfo
    {
        public long CreatedTick { get; } = Environment.TickCount64;

        public TaskCompletionSource TaskCompletionSource { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
