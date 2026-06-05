using MongoDB.Driver;
using Testcontainers.MongoDb;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.IntegrationTests;

internal sealed class MongoDbContainerFixture : IAsyncDisposable
{
    private readonly MongoDbContainer _container;

    private MongoDbContainerFixture(MongoDbContainer container)
    {
        _container = container;
        Client = new MongoClient(container.GetConnectionString());
        Database = Client.GetDatabase("signalr_" + Guid.NewGuid().ToString("N"));
    }

    public MongoClient Client { get; }

    public IMongoDatabase Database { get; }

    public static async Task<MongoDbContainerFixture> StartAsync(bool replicaSet)
    {
        var builder = new MongoDbBuilder("mongo:8.0");
        if (replicaSet)
        {
            builder = builder.WithReplicaSet("rs0");
        }

        var container = builder.Build();
        await container.StartAsync();
        return new MongoDbContainerFixture(container);
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DropDatabaseAsync(Database.DatabaseNamespace.DatabaseName);
        await _container.DisposeAsync();
    }
}
