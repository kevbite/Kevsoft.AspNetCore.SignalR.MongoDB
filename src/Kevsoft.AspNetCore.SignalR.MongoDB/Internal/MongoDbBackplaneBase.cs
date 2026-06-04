using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal abstract class MongoDbBackplaneBase : IMongoSignalRBackplane, IMongoDbSignalRCollectionInitializer
{
    private readonly SemaphoreSlim _startLock = new(1, 1);
    private readonly MongoBackplaneDocumentSerializer _documentSerializer;
    private readonly MongoBackplaneSubscriptionRegistry _subscriptions = new();
    private readonly ConcurrentDictionary<string, string> _localConnections = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _disposeTokenSource = new();
    private readonly TimeSpan _presenceHeartbeatPeriod;
    private Task? _readTask;
    private Task? _presenceTask;
    private bool _started;
    private int _disposed;

    protected MongoDbBackplaneBase(
        IMongoDatabase database,
        MongoDbSignalROptions options,
        IBackplaneEnvelopeSerializer envelopeSerializer,
        ILogger logger)
    {
        Database = database;
        Options = options;
        Logger = logger;
        Collection = database.GetCollection<BsonDocument>(options.CollectionName);
        PresenceCollection = database.GetCollection<BsonDocument>(options.CollectionName + "_connections");
        CheckpointStore = options.CheckpointStore ?? new InMemoryMessageCheckpointStore();
        _documentSerializer = new MongoBackplaneDocumentSerializer(envelopeSerializer);
        _presenceHeartbeatPeriod = TimeSpan.FromMilliseconds(Math.Max(5000, options.ConnectionPresenceTtl.TotalMilliseconds / 3));
    }

    protected IMongoDatabase Database { get; }

    protected IMongoCollection<BsonDocument> Collection { get; }

    protected IMongoCollection<BsonDocument> PresenceCollection { get; }

    protected MongoDbSignalROptions Options { get; }

    protected IMessageCheckpointStore CheckpointStore { get; }

    protected ILogger Logger { get; }

    protected string StreamId { get; private set; } = string.Empty;

    public async ValueTask StartAsync(string streamId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        if (_started)
        {
            return;
        }

        await _startLock.WaitAsync(cancellationToken);
        try
        {
            if (_started)
            {
                return;
            }

            StreamId = streamId;

            if (Options.RunCollectionSetupOnStartup)
            {
                await InitializeAsync(cancellationToken);
            }

            var linkedToken = _disposeTokenSource.Token;
            var readerReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _readTask = Task.Run(() => RunReaderLoopAsync(readerReady, linkedToken), CancellationToken.None);
            _presenceTask = Task.Run(() => RunPresenceHeartbeatAsync(linkedToken), CancellationToken.None);
            await readerReady.Task.WaitAsync(cancellationToken);
            _started = true;
        }
        finally
        {
            _startLock.Release();
        }
    }

    public async ValueTask<long> PublishAsync(MongoBackplaneEnvelope envelope, CancellationToken cancellationToken = default)
    {
        EnsureStarted();

        var document = _documentSerializer.Serialize(StreamId, envelope);
        await Collection.InsertOneAsync(document, cancellationToken: cancellationToken);
        return 1;
    }

    public ValueTask<IAsyncDisposable> SubscribeAsync(
        string channel,
        Func<MongoBackplaneEnvelope, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(_subscriptions.Add(channel, handler));
    }

    public async ValueTask AddConnectionAsync(string connectionId, string serverId, CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        _localConnections[connectionId] = serverId;

        await UpsertPresenceAsync(connectionId, serverId, cancellationToken);
    }

    public async ValueTask RemoveConnectionAsync(string connectionId, string serverId, CancellationToken cancellationToken = default)
    {
        _localConnections.TryRemove(connectionId, out _);

        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.StreamId, StreamId),
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.ConnectionId, connectionId),
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.ServerId, serverId));

        await PresenceCollection.DeleteOneAsync(filter, cancellationToken);
    }

    public async ValueTask<bool> HasConnectionAsync(string connectionId, CancellationToken cancellationToken = default)
    {
        EnsureStarted();

        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.StreamId, StreamId),
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.ConnectionId, connectionId),
            Builders<BsonDocument>.Filter.Gt(MongoBackplaneDocumentFields.ExpiresAtUtc, DateTime.UtcNow));

        return await PresenceCollection.Find(filter).Limit(1).AnyAsync(cancellationToken);
    }

    public async ValueTask InitializeAsync(CancellationToken cancellationToken = default)
    {
        await InitializeMessageCollectionAsync(cancellationToken);
        await EnsurePresenceIndexesAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _disposeTokenSource.Cancel();

        try
        {
            if (_readTask != null)
            {
                await _readTask;
            }

            if (_presenceTask != null)
            {
                await _presenceTask;
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await RemoveLocalPresenceAsync(CancellationToken.None);
            _subscriptions.Clear();
            _disposeTokenSource.Dispose();
            _startLock.Dispose();
        }
    }

    protected abstract Task RunReaderLoopAsync(TaskCompletionSource readerReady, CancellationToken cancellationToken);

    protected abstract ValueTask InitializeMessageCollectionAsync(CancellationToken cancellationToken);

    protected async ValueTask<bool> CollectionExistsAsync(string collectionName, CancellationToken cancellationToken)
    {
        using var cursor = await Database.ListCollectionNamesAsync(cancellationToken: cancellationToken);
        var names = await cursor.ToListAsync(cancellationToken);
        return names.Any(name => string.Equals(name, collectionName, StringComparison.Ordinal));
    }

    protected async ValueTask DispatchDocumentAsync(BsonDocument document, CancellationToken cancellationToken)
    {
        if (!document.TryGetValue(MongoBackplaneDocumentFields.StreamId, out var streamIdValue) ||
            !string.Equals(streamIdValue.AsString, StreamId, StringComparison.Ordinal) ||
            !document.Contains(MongoBackplaneDocumentFields.Version))
        {
            return;
        }

        MongoBackplaneEnvelope envelope;
        try
        {
            envelope = _documentSerializer.Deserialize(document);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Skipping malformed MongoDB SignalR backplane document.");
            return;
        }

        await _subscriptions.DispatchAsync(envelope, cancellationToken);
    }

    protected ValueTask StoreCheckpointAsync(MongoDbBackplaneCheckpoint checkpoint, CancellationToken cancellationToken)
    {
        return CheckpointStore.SetAsync(StreamId, checkpoint, cancellationToken);
    }

    protected static DateTimeOffset? ReadCreatedAt(BsonDocument document)
    {
        return document.TryGetValue(MongoBackplaneDocumentFields.CreatedAtUtc, out var value) && value.IsValidDateTime
            ? new DateTimeOffset(value.ToUniversalTime(), TimeSpan.Zero)
            : null;
    }

    private async ValueTask EnsurePresenceIndexesAsync(CancellationToken cancellationToken)
    {
        if (!Options.CreateIndexes)
        {
            return;
        }

        var streamConnectionKeys = Builders<BsonDocument>.IndexKeys
            .Ascending(MongoBackplaneDocumentFields.StreamId)
            .Ascending(MongoBackplaneDocumentFields.ConnectionId);
        var streamConnectionOptions = new CreateIndexOptions { Name = "signalr_presence_stream_connection" };

        var expiresAtKeys = Builders<BsonDocument>.IndexKeys.Ascending(MongoBackplaneDocumentFields.ExpiresAtUtc);
        var expiresAtOptions = new CreateIndexOptions
        {
            Name = "signalr_presence_expires_at",
            ExpireAfter = TimeSpan.Zero
        };

        await PresenceCollection.Indexes.CreateManyAsync(
            [
                new CreateIndexModel<BsonDocument>(streamConnectionKeys, streamConnectionOptions),
                new CreateIndexModel<BsonDocument>(expiresAtKeys, expiresAtOptions)
            ],
            cancellationToken);
    }

    private async Task RunPresenceHeartbeatAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_presenceHeartbeatPeriod);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await timer.WaitForNextTickAsync(cancellationToken);
                foreach (var connection in _localConnections)
                {
                    await UpsertPresenceAsync(connection.Key, connection.Value, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Failed to refresh MongoDB SignalR connection presence.");
            }
        }
    }

    private Task UpsertPresenceAsync(string connectionId, string serverId, CancellationToken cancellationToken)
    {
        var expiresAt = DateTime.UtcNow.Add(Options.ConnectionPresenceTtl);
        var filter = Builders<BsonDocument>.Filter.And(
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.StreamId, StreamId),
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.ConnectionId, connectionId),
            Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.ServerId, serverId));
        var update = Builders<BsonDocument>.Update
            .Set(MongoBackplaneDocumentFields.StreamId, StreamId)
            .Set(MongoBackplaneDocumentFields.ConnectionId, connectionId)
            .Set(MongoBackplaneDocumentFields.ServerId, serverId)
            .Set(MongoBackplaneDocumentFields.ExpiresAtUtc, expiresAt);

        return PresenceCollection.UpdateOneAsync(
            filter,
            update,
            new UpdateOptions { IsUpsert = true },
            cancellationToken);
    }

    private async Task RemoveLocalPresenceAsync(CancellationToken cancellationToken)
    {
        foreach (var connection in _localConnections)
        {
            await RemoveConnectionAsync(connection.Key, connection.Value, cancellationToken);
        }
    }

    private void EnsureStarted()
    {
        if (!_started)
        {
            throw new InvalidOperationException("The MongoDB SignalR backplane has not been started.");
        }
    }
}
