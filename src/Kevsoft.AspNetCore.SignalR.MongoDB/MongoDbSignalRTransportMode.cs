namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Specifies the MongoDB mechanism used to observe backplane messages.
/// </summary>
public enum MongoDbSignalRTransportMode
{
    /// <summary>
    /// Uses MongoDB change streams. This requires MongoDB to run as a replica set or sharded cluster.
    /// </summary>
    ChangeStreams,

    /// <summary>
    /// Uses a tailable-await cursor over a capped collection.
    /// </summary>
    TailableAwait
}
