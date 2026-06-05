using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.IntegrationTests;

public sealed class TailableAwaitContainerFixture : IAsyncLifetime
{
    private MongoDbContainerFixture? _fixture;

    public IMongoDatabase? Database => _fixture?.Database;

    public async Task InitializeAsync()
    {
        if (DockerFactAttribute.IsDockerAvailable())
        {
            _fixture = await MongoDbContainerFixture.StartAsync(replicaSet: false);
        }
    }

    public async Task DisposeAsync()
    {
        if (_fixture is not null)
        {
            await _fixture.DisposeAsync();
        }
    }
}
