namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Stores transient MongoDB backplane checkpoints.
/// </summary>
/// <remarks>
/// Implementations are intended for in-process recovery from cursor interruptions. They should not be used to replay stale
/// SignalR messages after a process cold start unless a future durable replay design explicitly supports it.
/// </remarks>
public interface IMessageCheckpointStore
{
    /// <summary>
    /// Gets the checkpoint for the specified stream.
    /// </summary>
    /// <param name="streamId">The logical stream identifier.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    /// <returns>The stored checkpoint, or <see langword="null"/> when no checkpoint exists.</returns>
    ValueTask<MongoDbBackplaneCheckpoint?> GetAsync(string streamId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores the checkpoint for the specified stream.
    /// </summary>
    /// <param name="streamId">The logical stream identifier.</param>
    /// <param name="checkpoint">The checkpoint to store.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    ValueTask SetAsync(string streamId, MongoDbBackplaneCheckpoint checkpoint, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clears the checkpoint for the specified stream.
    /// </summary>
    /// <param name="streamId">The logical stream identifier.</param>
    /// <param name="cancellationToken">A token that can cancel the operation.</param>
    ValueTask ClearAsync(string streamId, CancellationToken cancellationToken = default);
}
