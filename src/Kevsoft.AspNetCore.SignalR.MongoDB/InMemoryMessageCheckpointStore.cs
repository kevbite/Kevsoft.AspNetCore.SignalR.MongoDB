using System.Collections.Concurrent;

namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Stores MongoDB backplane checkpoints in process memory.
/// </summary>
public sealed class InMemoryMessageCheckpointStore : IMessageCheckpointStore
{
    private readonly ConcurrentDictionary<string, MongoDbBackplaneCheckpoint> _checkpoints = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public ValueTask<MongoDbBackplaneCheckpoint?> GetAsync(string streamId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        cancellationToken.ThrowIfCancellationRequested();

        _checkpoints.TryGetValue(streamId, out var checkpoint);
        return ValueTask.FromResult<MongoDbBackplaneCheckpoint?>(checkpoint);
    }

    /// <inheritdoc />
    public ValueTask SetAsync(
        string streamId,
        MongoDbBackplaneCheckpoint checkpoint,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        ArgumentNullException.ThrowIfNull(checkpoint);
        cancellationToken.ThrowIfCancellationRequested();

        _checkpoints[streamId] = checkpoint;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask ClearAsync(string streamId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);
        cancellationToken.ThrowIfCancellationRequested();

        _checkpoints.TryRemove(streamId, out _);
        return ValueTask.CompletedTask;
    }
}
