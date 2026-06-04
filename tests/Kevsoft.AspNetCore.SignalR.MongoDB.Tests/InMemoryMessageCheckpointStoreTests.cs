using Kevsoft.AspNetCore.SignalR.MongoDB;
using MongoDB.Bson;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class InMemoryMessageCheckpointStoreTests
{
    [Fact]
    public async Task GetAsyncReturnsNullWhenCheckpointDoesNotExist()
    {
        var store = new InMemoryMessageCheckpointStore();

        var checkpoint = await store.GetAsync("stream");

        Assert.Null(checkpoint);
    }

    [Fact]
    public async Task SetAsyncStoresCheckpointByStreamId()
    {
        var store = new InMemoryMessageCheckpointStore();
        var checkpoint = MongoDbBackplaneCheckpoint.ForTailable(ObjectId.GenerateNewId());

        await store.SetAsync("stream", checkpoint);

        Assert.Same(checkpoint, await store.GetAsync("stream"));
    }

    [Fact]
    public async Task ClearAsyncRemovesStoredCheckpoint()
    {
        var store = new InMemoryMessageCheckpointStore();
        var checkpoint = MongoDbBackplaneCheckpoint.ForTailable(ObjectId.GenerateNewId());

        await store.SetAsync("stream", checkpoint);
        await store.ClearAsync("stream");

        Assert.Null(await store.GetAsync("stream"));
    }
}
