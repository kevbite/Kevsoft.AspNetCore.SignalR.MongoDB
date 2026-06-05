using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.IntegrationTests;

[Trait("Category", "Integration")]
public class ChangeStreamScaleoutTests(ChangeStreamContainerFixture fixture)
    : MongoDbScaleoutTestsBase, IClassFixture<ChangeStreamContainerFixture>
{
    protected override MongoDbHubLifetimeManager<Hub> CreateManager(string collectionName)
    {
        var options = new MongoDbSignalROptions
        {
            CollectionName = collectionName,
            AckTimeout = TimeSpan.FromSeconds(15),
            ConnectionPresenceTtl = TimeSpan.FromMinutes(1),
            CheckpointStore = new InMemoryMessageCheckpointStore(),
        };
        options.UseChangeStreams(o => o.MessageTtl = TimeSpan.FromMinutes(5));

        var backplane = new MongoDbChangeStreamBackplane(
            fixture.Database!,
            options,
            CreateEnvelopeSerializer(),
            NullLogger<MongoDbChangeStreamBackplane>.Instance);

        return CreateManagerFromBackplane(backplane, options);
    }
}
