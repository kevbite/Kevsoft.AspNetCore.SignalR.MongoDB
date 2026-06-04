using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB;

/// <summary>
/// Options used to configure MongoDB scale-out for ASP.NET Core SignalR.
/// </summary>
public sealed class MongoDbSignalROptions
{
    /// <summary>
    /// The default collection name used to store backplane messages.
    /// </summary>
    public const string DefaultCollectionName = "signalr_messages";

    /// <summary>
    /// The default capped collection size used by the tailable-await transport.
    /// </summary>
    public const int DefaultTailableCollectionSizeBytes = 64 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the MongoDB connection string.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Gets or sets the MongoDB database name that contains the backplane collection.
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Gets or sets the MongoDB collection name used for backplane messages.
    /// </summary>
    public string CollectionName { get; set; } = DefaultCollectionName;

    /// <summary>
    /// Gets or sets the MongoDB transport mode.
    /// </summary>
    public MongoDbSignalRTransportMode TransportMode { get; set; } = MongoDbSignalRTransportMode.ChangeStreams;

    /// <summary>
    /// Gets or sets a factory for creating or resolving the MongoDB client.
    /// </summary>
    public Func<IServiceProvider, IMongoClient>? MongoClientFactory { get; set; }

    /// <summary>
    /// Gets or sets a callback used to configure client settings created from <see cref="ConnectionString"/>.
    /// </summary>
    public Action<MongoClientSettings>? ConfigureClientSettings { get; set; }

    /// <summary>
    /// Gets or sets the optional channel prefix used to isolate applications sharing a MongoDB collection.
    /// </summary>
    public string? ChannelPrefix { get; set; }

    /// <summary>
    /// Gets or sets the timeout used when waiting for backplane acknowledgement messages.
    /// </summary>
    public TimeSpan AckTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum server await time for tailable-await cursor operations.
    /// </summary>
    public TimeSpan TailableAwaitMaxAwaitTime { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the capped collection size, in bytes, for the tailable-await transport.
    /// </summary>
    public int TailableCollectionSizeBytes { get; set; } = DefaultTailableCollectionSizeBytes;

    /// <summary>
    /// Gets or sets how long backplane message documents should be retained for cleanup-capable transports.
    /// </summary>
    public TimeSpan MessageTtl { get; set; } = TimeSpan.FromDays(1);

    /// <summary>
    /// Gets or sets how long connection presence records are considered valid without a heartbeat.
    /// </summary>
    public TimeSpan ConnectionPresenceTtl { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Gets or sets the checkpoint store used for transient in-process cursor recovery.
    /// </summary>
    /// <remarks>
    /// Checkpoints are not used to replay historical SignalR messages after process cold start.
    /// </remarks>
    public IMessageCheckpointStore? CheckpointStore { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the library should create the message collection when it is missing.
    /// </summary>
    public bool CreateCollectionIfMissing { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether the library should create required MongoDB indexes when supported.
    /// </summary>
    public bool CreateIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether collection setup should run automatically during startup.
    /// </summary>
    public bool RunCollectionSetupOnStartup { get; set; } = true;
}
