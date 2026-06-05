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

    // -------------------------------------------------------------------------
    // Connection properties – set via Use* methods.
    // -------------------------------------------------------------------------

    /// <summary>
    /// Gets the MongoDB connection string. Set via
    /// <see cref="UseConnectionString(string)"/> or <see cref="UseConnectionString(string, string, Action{MongoClientSettings}?)"/>.
    /// </summary>
    public string? ConnectionString { get; private set; }

    /// <summary>
    /// Gets the MongoDB database name. Set via
    /// <see cref="UseConnectionString(string)"/>, <see cref="UseConnectionString(string, string, Action{MongoClientSettings}?)"/>,
    /// or <see cref="UseMongoClient(Func{IServiceProvider, IMongoClient}, string)"/>.
    /// </summary>
    public string? DatabaseName { get; private set; }

    /// <summary>
    /// Gets the factory for creating or resolving the MongoDB client. Set via
    /// <see cref="UseMongoClient(Func{IServiceProvider, IMongoClient}, string)"/>.
    /// </summary>
    public Func<IServiceProvider, IMongoClient>? MongoClientFactory { get; private set; }

    /// <summary>
    /// Gets the factory for creating or resolving an <see cref="IMongoDatabase"/> directly.
    /// When set, <see cref="ConnectionString"/>, <see cref="DatabaseName"/>, and
    /// <see cref="MongoClientFactory"/> are all ignored for database resolution purposes.
    /// Set via <see cref="UseMongoDatabase(Func{IServiceProvider, IMongoDatabase})"/>.
    /// </summary>
    public Func<IServiceProvider, IMongoDatabase>? MongoDatabaseFactory { get; private set; }

    /// <summary>
    /// Gets the callback used to configure client settings when building a client from
    /// <see cref="ConnectionString"/>. Set via
    /// <see cref="UseConnectionString(string, string, Action{MongoClientSettings}?)"/>.
    /// </summary>
    public Action<MongoClientSettings>? ConfigureClientSettings { get; private set; }

    // -------------------------------------------------------------------------
    // Transport properties – set via UseChangeStreams / UseTailableAwait.
    // -------------------------------------------------------------------------

    /// <summary>
    /// Gets the MongoDB transport mode. Set via <see cref="UseChangeStreams(Action{MongoDbSignalRChangeStreamOptions}?)"/>
    /// or <see cref="UseTailableAwait(Action{MongoDbSignalRTailableAwaitOptions}?)"/>.
    /// Defaults to <see cref="MongoDbSignalRTransportMode.ChangeStreams"/> when neither method is called.
    /// </summary>
    public MongoDbSignalRTransportMode TransportMode { get; private set; } = MongoDbSignalRTransportMode.ChangeStreams;

    /// <summary>
    /// Gets how long backplane message documents are retained for cleanup-capable transports.
    /// Set via <see cref="UseChangeStreams(Action{MongoDbSignalRChangeStreamOptions}?)"/>.
    /// </summary>
    public TimeSpan MessageTtl { get; private set; } = TimeSpan.FromDays(1);

    /// <summary>
    /// Gets the maximum server await time for tailable-await cursor operations.
    /// Set via <see cref="UseTailableAwait(Action{MongoDbSignalRTailableAwaitOptions}?)"/>.
    /// </summary>
    public TimeSpan TailableAwaitMaxAwaitTime { get; private set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets the capped collection size in bytes for the tailable-await transport.
    /// Set via <see cref="UseTailableAwait(Action{MongoDbSignalRTailableAwaitOptions}?)"/>.
    /// </summary>
    public int TailableCollectionSizeBytes { get; private set; } = DefaultTailableCollectionSizeBytes;

    // -------------------------------------------------------------------------
    // General properties – applicable to all transport modes and connection paths.
    // -------------------------------------------------------------------------

    /// <summary>
    /// Gets or sets the MongoDB collection name used for backplane messages.
    /// </summary>
    public string CollectionName { get; set; } = DefaultCollectionName;

    /// <summary>
    /// Gets or sets the optional channel prefix used to isolate applications sharing a MongoDB collection.
    /// It is combined with the hub type so multiple hubs in the same application stay isolated.
    /// </summary>
    public string? ChannelPrefix { get; set; }

    /// <summary>
    /// Gets or sets the timeout used when waiting for backplane acknowledgement messages.
    /// </summary>
    public TimeSpan AckTimeout { get; set; } = TimeSpan.FromSeconds(30);

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

    // -------------------------------------------------------------------------
    // Connection configuration methods.
    // -------------------------------------------------------------------------

    /// <summary>
    /// Configures the backplane to connect using the supplied MongoDB connection string.
    /// If the connection string includes a database name it is used as <see cref="DatabaseName"/>;
    /// otherwise <see cref="DatabaseName"/> must be set by calling
    /// <see cref="UseConnectionString(string, string, Action{MongoClientSettings}?)"/> instead.
    /// </summary>
    /// <param name="connectionString">A valid MongoDB connection string.</param>
    public void UseConnectionString(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        ConnectionString = connectionString;

        if (string.IsNullOrWhiteSpace(DatabaseName))
        {
            var mongoUrl = MongoUrl.Create(connectionString);
            if (!string.IsNullOrWhiteSpace(mongoUrl.DatabaseName))
            {
                DatabaseName = mongoUrl.DatabaseName;
            }
        }
    }

    /// <summary>
    /// Configures the backplane to connect using the supplied MongoDB connection string and database name.
    /// </summary>
    /// <param name="connectionString">A valid MongoDB connection string.</param>
    /// <param name="databaseName">The MongoDB database that contains the backplane collection.</param>
    /// <param name="configureSettings">
    /// An optional callback to further configure the <see cref="MongoClientSettings"/> built from
    /// <paramref name="connectionString"/>.
    /// </param>
    public void UseConnectionString(string connectionString, string databaseName,
        Action<MongoClientSettings>? configureSettings = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);

        ConnectionString = connectionString;
        DatabaseName = databaseName;
        ConfigureClientSettings = configureSettings;
    }

    /// <summary>
    /// Configures the backplane to obtain its <see cref="IMongoClient"/> from the supplied factory,
    /// and to use the named database within that client.
    /// </summary>
    /// <param name="factory">
    /// A factory that creates or resolves the <see cref="IMongoClient"/>. The
    /// <see cref="IServiceProvider"/> is the root (singleton-scope) service provider; do not
    /// resolve scoped services from it.
    /// </param>
    /// <param name="databaseName">The MongoDB database that contains the backplane collection.</param>
    public void UseMongoClient(Func<IServiceProvider, IMongoClient> factory, string databaseName)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);

        MongoClientFactory = factory;
        DatabaseName = databaseName;
    }

    /// <summary>
    /// Configures the backplane to obtain its <see cref="IMongoDatabase"/> directly from the
    /// supplied factory, bypassing <see cref="ConnectionString"/>, <see cref="DatabaseName"/>,
    /// and <see cref="MongoClientFactory"/> entirely.
    /// </summary>
    /// <param name="factory">
    /// A factory that creates or resolves the <see cref="IMongoDatabase"/>. The
    /// <see cref="IServiceProvider"/> is the root (singleton-scope) service provider; do not
    /// resolve scoped services from it.
    /// </param>
    public void UseMongoDatabase(Func<IServiceProvider, IMongoDatabase> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);

        MongoDatabaseFactory = factory;
    }

    // -------------------------------------------------------------------------
    // Transport configuration methods.
    // -------------------------------------------------------------------------

    /// <summary>
    /// Configures the backplane to use change streams as the transport.
    /// Requires MongoDB to run as a replica set or sharded cluster.
    /// </summary>
    /// <param name="configure">
    /// An optional callback to configure change-stream-specific options such as
    /// <see cref="MongoDbSignalRChangeStreamOptions.MessageTtl"/>.
    /// </param>
    public void UseChangeStreams(Action<MongoDbSignalRChangeStreamOptions>? configure = null)
    {
        TransportMode = MongoDbSignalRTransportMode.ChangeStreams;

        var opts = new MongoDbSignalRChangeStreamOptions { MessageTtl = MessageTtl };
        configure?.Invoke(opts);
        MessageTtl = opts.MessageTtl;
    }

    /// <summary>
    /// Configures the backplane to use a tailable-await cursor over a capped collection as the transport.
    /// Compatible with standalone MongoDB deployments.
    /// </summary>
    /// <param name="configure">
    /// An optional callback to configure tailable-await-specific options such as
    /// <see cref="MongoDbSignalRTailableAwaitOptions.MaxAwaitTime"/> and
    /// <see cref="MongoDbSignalRTailableAwaitOptions.CollectionSizeBytes"/>.
    /// </param>
    public void UseTailableAwait(Action<MongoDbSignalRTailableAwaitOptions>? configure = null)
    {
        TransportMode = MongoDbSignalRTransportMode.TailableAwait;

        var opts = new MongoDbSignalRTailableAwaitOptions
        {
            MaxAwaitTime = TailableAwaitMaxAwaitTime,
            CollectionSizeBytes = TailableCollectionSizeBytes
        };
        configure?.Invoke(opts);
        TailableAwaitMaxAwaitTime = opts.MaxAwaitTime;
        TailableCollectionSizeBytes = opts.CollectionSizeBytes;
    }
}

