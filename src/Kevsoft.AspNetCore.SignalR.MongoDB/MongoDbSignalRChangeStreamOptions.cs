namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Options specific to the change-stream transport mode.
/// Configure this via <see cref="MongoDbSignalROptions.UseChangeStreams(Action{MongoDbSignalRChangeStreamOptions}?)"/>.
/// </summary>
public sealed class MongoDbSignalRChangeStreamOptions
{
    /// <summary>
    /// Gets or sets how long backplane message documents should be retained.
    /// Used to configure a TTL index on the backplane collection.
    /// </summary>
    public TimeSpan MessageTtl { get; set; } = TimeSpan.FromDays(1);
}
