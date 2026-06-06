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
                // Build a pipeline to filter server-side, reducing network overhead.
                // Only watch Insert operations and filter by streamId to avoid pulling
                // documents for other applications or hubs sharing the same collection.
                var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
                    .Match(Builders<ChangeStreamDocument<BsonDocument>>.Filter.And(
                        Builders<ChangeStreamDocument<BsonDocument>>.Filter.Eq(x => x.OperationType, ChangeStreamOperationType.Insert),
                        Builders<ChangeStreamDocument<BsonDocument>>.Filter.Eq($"fullDocument.{MongoBackplaneDocumentFields.StreamId}", StreamId)));

                var options = new ChangeStreamOptions
                {
                    ResumeAfter = _resumeToken
                };

                using var cursor = await Collection.WatchAsync(pipeline, options, cancellationToken);
                readerReady.TrySetResult();
                while (!cancellationToken.IsCancellationRequested && await cursor.MoveNextAsync(cancellationToken))
                {
                    foreach (var change in cursor.Current)
                    {
                        // The pipeline guarantees FullDocument is non-null for Insert operations.
                        await DispatchDocumentAsync(change.FullDocument!, cancellationToken);

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
        // Three MongoDB error codes indicate the change stream cannot be resumed with
        // the current token and must be restarted from the current live position:
        //   260 = InvalidResumeToken    – the token is structurally malformed or invalid
        //   280 = ChangeStreamFatalError – a non-resumable fatal stream condition
        //   286 = ChangeStreamHistoryLost – the oplog has been rolled past the resume point
        // Both code number and code name are checked for defensive coverage.
        return ex.Code is 260 or 280 or 286 ||
            string.Equals(ex.CodeName, "InvalidResumeToken", StringComparison.Ordinal) ||
            string.Equals(ex.CodeName, "ChangeStreamFatalError", StringComparison.Ordinal) ||
            string.Equals(ex.CodeName, "ChangeStreamHistoryLost", StringComparison.Ordinal);
    }
}
