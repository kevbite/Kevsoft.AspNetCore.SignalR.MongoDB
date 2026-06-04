using Kevsoft.AspNetCore.SignalR.MongoDB;
using MongoDB.Bson;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoDbBackplaneCheckpointTests
{
    [Fact]
    public void ForChangeStreamCopiesResumeToken()
    {
        var resumeToken = new BsonDocument("_data", "token");
        var checkpoint = MongoDbBackplaneCheckpoint.ForChangeStream(resumeToken);

        resumeToken["_data"] = "mutated";

        Assert.Equal("token", checkpoint.GetChangeStreamResumeToken()!["_data"].AsString);
    }

    [Fact]
    public void GetChangeStreamResumeTokenReturnsCopy()
    {
        var checkpoint = MongoDbBackplaneCheckpoint.ForChangeStream(new BsonDocument("_data", "token"));

        var resumeToken = checkpoint.GetChangeStreamResumeToken()!;
        resumeToken["_data"] = "mutated";

        Assert.Equal("token", checkpoint.GetChangeStreamResumeToken()!["_data"].AsString);
    }

    [Fact]
    public void ForTailableStoresPositionAndTimestamp()
    {
        var position = ObjectId.GenerateNewId();
        var timestamp = DateTimeOffset.UtcNow;

        var checkpoint = MongoDbBackplaneCheckpoint.ForTailable(position, timestamp);

        Assert.Equal(position, checkpoint.TailablePosition);
        Assert.Equal(timestamp, checkpoint.TailableTimestamp);
        Assert.Null(checkpoint.GetChangeStreamResumeToken());
    }
}
