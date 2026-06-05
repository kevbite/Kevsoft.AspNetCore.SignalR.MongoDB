namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Options specific to the tailable-await transport mode.
/// Configure this via <see cref="MongoDbSignalROptions.UseTailableAwait(Action{MongoDbSignalRTailableAwaitOptions}?)"/>.
/// </summary>
public sealed class MongoDbSignalRTailableAwaitOptions
{
    /// <summary>
    /// Gets or sets the maximum server-side wait time for a tailable-await cursor before it returns
    /// an empty batch. Shorter values reduce idle latency; longer values reduce server round-trips.
    /// </summary>
    public TimeSpan MaxAwaitTime { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the capped collection size in bytes. Size it for the largest outage or burst
    /// you need to tolerate: <c>average message bytes × messages per second × tolerated outage seconds</c>,
    /// plus headroom for indexes and message-size spikes.
    /// </summary>
    public int CollectionSizeBytes { get; set; } = MongoDbSignalROptions.DefaultTailableCollectionSizeBytes;
}
