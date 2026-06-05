using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.IntegrationTests;

[Trait("Category", "Integration")]
public class TailableAwaitScaleoutTests(TailableAwaitContainerFixture fixture)
    : MongoDbScaleoutTestsBase, IClassFixture<TailableAwaitContainerFixture>
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
        options.UseTailableAwait(o =>
        {
            o.MaxAwaitTime = TimeSpan.FromMilliseconds(100);
            o.CollectionSizeBytes = 1024 * 1024;
        });

        var backplane = new MongoDbTailableAwaitBackplane(
            fixture.Database!,
            options,
            CreateEnvelopeSerializer(),
            NullLogger<MongoDbTailableAwaitBackplane>.Instance);

        return CreateManagerFromBackplane(backplane, options);
    }
}
