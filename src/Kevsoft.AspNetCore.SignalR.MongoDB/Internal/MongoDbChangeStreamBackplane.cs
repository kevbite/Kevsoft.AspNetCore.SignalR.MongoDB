using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoDbChangeStreamBackplane : MongoDbBackplaneBase
{
    private BsonDocument? _resumeToken;

    public MongoDbChangeStreamBackplane(
        IMongoDatabase database,
        MongoDbSignalROptions options,
        IBackplaneEnvelopeSerializer envelopeSerializer,
        ILogger<MongoDbChangeStreamBackplane> logger)
        : base(database, options, envelopeSerializer, logger)
    {
    }

    protected override async ValueTask InitializeMessageCollectionAsync(CancellationToken cancellationToken)
    {
        var exists = await CollectionExistsAsync(Options.CollectionName, cancellationToken);
        if (!exists)
        {
            if (!Options.CreateCollectionIfMissing)
            {
                throw new InvalidOperationException(
                    $"MongoDB SignalR collection '{Options.CollectionName}' does not exist.");
            }

            await Database.CreateCollectionAsync(Options.CollectionName, cancellationToken: cancellationToken);
        }

        if (!Options.CreateIndexes)
        {
            return;
        }

        var ttlIndex = new CreateIndexModel<BsonDocument>(
            Builders<BsonDocument>.IndexKeys.Ascending(MongoBackplaneDocumentFields.CreatedAtUtc),
            new CreateIndexOptions
            {
                Name = "signalr_messages_created_at_ttl",
                ExpireAfter = Options.MessageTtl
            });

        var streamIndex = new CreateIndexModel<BsonDocument>(
            Builders<BsonDocument>.IndexKeys
                .Ascending(MongoBackplaneDocumentFields.StreamId)
                .Ascending(MongoBackplaneDocumentFields.CreatedAtUtc),
            new CreateIndexOptions { Name = "signalr_messages_stream_created_at" });

        await Collection.Indexes.CreateManyAsync([ttlIndex, streamIndex], cancellationToken);
    }

    protected override async Task RunReaderLoopAsync(TaskCompletionSource readerReady, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = new ChangeStreamOptions
                {
                    ResumeAfter = _resumeToken
                };

                using var cursor = await Collection.WatchAsync(options, cancellationToken);
                readerReady.TrySetResult();
                while (!cancellationToken.IsCancellationRequested && await cursor.MoveNextAsync(cancellationToken))
                {
                    foreach (var change in cursor.Current)
                    {
                        if (change.OperationType != ChangeStreamOperationType.Insert || change.FullDocument == null)
                        {
                            continue;
                        }

                        await DispatchDocumentAsync(change.FullDocument, cancellationToken);

                        if (change.ResumeToken != null)
                        {
                            _resumeToken = change.ResumeToken;
                            await StoreCheckpointAsync(
                                MongoDbBackplaneCheckpoint.ForChangeStream(change.ResumeToken),
                                cancellationToken);
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (MongoCommandException ex) when (IsInvalidResumeToken(ex))
            {
                if (TryFailStartup(readerReady, ex))
                {
                    return;
                }

                Logger.LogWarning(ex, "MongoDB SignalR change-stream resume token is no longer valid; restarting from live position.");
                _resumeToken = null;
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }
            catch (Exception ex)
            {
                if (TryFailStartup(readerReady, ex))
                {
                    return;
                }

                Logger.LogWarning(ex, "MongoDB SignalR change stream interrupted; reconnecting.");
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }
        }
    }

    private static bool IsInvalidResumeToken(MongoCommandException ex)
    {
        return ex.Code == 286 ||
            string.Equals(ex.CodeName, "ChangeStreamHistoryLost", StringComparison.Ordinal) ||
            ex.Message.Contains("resume token", StringComparison.OrdinalIgnoreCase);
    }
}
