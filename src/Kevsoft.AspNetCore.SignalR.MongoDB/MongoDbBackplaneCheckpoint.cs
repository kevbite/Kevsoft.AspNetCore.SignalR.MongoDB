using MongoDB.Bson;

namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Represents the last observed MongoDB backplane position for transient in-process recovery.
/// </summary>
/// <remarks>
/// SignalR backplane messages are ephemeral. Checkpoints must not be used to replay old messages after a process cold start.
/// </remarks>
public sealed class MongoDbBackplaneCheckpoint
{
    private readonly BsonDocument? _changeStreamResumeToken;

    private MongoDbBackplaneCheckpoint(
        BsonDocument? changeStreamResumeToken,
        BsonValue? tailablePosition,
        DateTimeOffset? tailableTimestamp,
        DateTimeOffset storedAt)
    {
        _changeStreamResumeToken = changeStreamResumeToken;
        TailablePosition = tailablePosition;
        TailableTimestamp = tailableTimestamp;
        StoredAt = storedAt;
    }

    /// <summary>
    /// Gets the tailable cursor position for transports that use a capped collection.
    /// </summary>
    public BsonValue? TailablePosition { get; }

    /// <summary>
    /// Gets the timestamp associated with the tailable cursor position, when available.
    /// </summary>
    public DateTimeOffset? TailableTimestamp { get; }

    /// <summary>
    /// Gets the time this checkpoint was stored.
    /// </summary>
    public DateTimeOffset StoredAt { get; }

    /// <summary>
    /// Creates a checkpoint for a change stream resume token.
    /// </summary>
    /// <param name="resumeToken">The MongoDB change stream resume token.</param>
    /// <param name="storedAt">The time the checkpoint was stored. Defaults to the current UTC time.</param>
    /// <returns>A checkpoint containing a change stream resume token.</returns>
    public static MongoDbBackplaneCheckpoint ForChangeStream(BsonDocument resumeToken, DateTimeOffset? storedAt = null)
    {
        ArgumentNullException.ThrowIfNull(resumeToken);

        return new MongoDbBackplaneCheckpoint(
            (BsonDocument)resumeToken.DeepClone(),
            tailablePosition: null,
            tailableTimestamp: null,
            storedAt ?? DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// Creates a checkpoint for a tailable cursor position.
    /// </summary>
    /// <param name="position">The last observed tailable cursor position.</param>
    /// <param name="timestamp">The timestamp associated with the position, when available.</param>
    /// <param name="storedAt">The time the checkpoint was stored. Defaults to the current UTC time.</param>
    /// <returns>A checkpoint containing a tailable cursor position.</returns>
    public static MongoDbBackplaneCheckpoint ForTailable(
        BsonValue position,
        DateTimeOffset? timestamp = null,
        DateTimeOffset? storedAt = null)
    {
        ArgumentNullException.ThrowIfNull(position);

        return new MongoDbBackplaneCheckpoint(
            changeStreamResumeToken: null,
            tailablePosition: position,
            tailableTimestamp: timestamp,
            storedAt ?? DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// Gets a copy of the change stream resume token.
    /// </summary>
    /// <returns>The resume token, or <see langword="null"/> when this checkpoint is not for a change stream.</returns>
    public BsonDocument? GetChangeStreamResumeToken()
    {
        return _changeStreamResumeToken is null ? null : (BsonDocument)_changeStreamResumeToken.DeepClone();
    }
}
