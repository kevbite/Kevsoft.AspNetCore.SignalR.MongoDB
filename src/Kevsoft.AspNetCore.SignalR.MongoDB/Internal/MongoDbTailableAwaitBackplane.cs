using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Internal;

internal sealed class MongoDbTailableAwaitBackplane : MongoDbBackplaneBase
{
    private BsonValue? _lastTailablePosition;

    public MongoDbTailableAwaitBackplane(
        IMongoDatabase database,
        MongoDbSignalROptions options,
        IBackplaneEnvelopeSerializer envelopeSerializer,
        ILogger<MongoDbTailableAwaitBackplane> logger)
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
                    $"MongoDB SignalR capped collection '{Options.CollectionName}' does not exist.");
            }

            await Database.CreateCollectionAsync(
                Options.CollectionName,
                new CreateCollectionOptions
                {
                    Capped = true,
                    MaxSize = Options.TailableCollectionSizeBytes
                },
                cancellationToken);
        }

        await ValidateCappedCollectionAsync(cancellationToken);
        await EnsureSentinelDocumentAsync(cancellationToken);
    }

    protected override async Task RunReaderLoopAsync(CancellationToken cancellationToken)
    {
        _lastTailablePosition = await ReadCurrentEndPositionAsync(cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var filter = BuildTailFilter();
                var options = new FindOptions<BsonDocument>
                {
                    CursorType = CursorType.TailableAwait,
                    MaxAwaitTime = Options.TailableAwaitMaxAwaitTime
                };

                using var cursor = await Collection.FindAsync(filter, options, cancellationToken);
                while (!cancellationToken.IsCancellationRequested && await cursor.MoveNextAsync(cancellationToken))
                {
                    foreach (var document in cursor.Current)
                    {
                        await DispatchDocumentAsync(document, cancellationToken);

                        if (document.TryGetValue(MongoBackplaneDocumentFields.Id, out var id))
                        {
                            _lastTailablePosition = id;
                            await StoreCheckpointAsync(
                                MongoDbBackplaneCheckpoint.ForTailable(id, ReadCreatedAt(document)),
                                cancellationToken);
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "MongoDB SignalR tailable-await cursor interrupted; reconnecting.");
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }
        }
    }

    private FilterDefinition<BsonDocument> BuildTailFilter()
    {
        var filter = Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.StreamId, StreamId);
        if (_lastTailablePosition != null)
        {
            filter &= Builders<BsonDocument>.Filter.Gt(MongoBackplaneDocumentFields.Id, _lastTailablePosition);
        }

        return filter;
    }

    private async Task<BsonValue?> ReadCurrentEndPositionAsync(CancellationToken cancellationToken)
    {
        var filter = Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.StreamId, StreamId);
        var document = await Collection
            .Find(filter)
            .Sort(new BsonDocument("$natural", -1))
            .Limit(1)
            .FirstOrDefaultAsync(cancellationToken);

        return document?.GetValue(MongoBackplaneDocumentFields.Id, null);
    }

    private async ValueTask ValidateCappedCollectionAsync(CancellationToken cancellationToken)
    {
        var command = new BsonDocument("collStats", Options.CollectionName);
        var stats = await Database.RunCommandAsync<BsonDocument>(command, cancellationToken: cancellationToken);
        if (!stats.TryGetValue("capped", out var capped) || !capped.ToBoolean())
        {
            throw new InvalidOperationException(
                $"MongoDB SignalR collection '{Options.CollectionName}' must be capped for the tailable-await transport.");
        }
    }

    private async ValueTask EnsureSentinelDocumentAsync(CancellationToken cancellationToken)
    {
        var filter = Builders<BsonDocument>.Filter.Eq(MongoBackplaneDocumentFields.StreamId, StreamId);
        if (await Collection.Find(filter).Limit(1).AnyAsync(cancellationToken))
        {
            return;
        }

        await Collection.InsertOneAsync(
            new BsonDocument
            {
                [MongoBackplaneDocumentFields.StreamId] = StreamId,
                ["sentinel"] = true,
                [MongoBackplaneDocumentFields.CreatedAtUtc] = DateTime.UtcNow
            },
            cancellationToken: cancellationToken);
    }
}
