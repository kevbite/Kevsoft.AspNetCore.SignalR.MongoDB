using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.AspNetCore.SignalR.Specification.Tests;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoDbScaleoutHubLifetimeManagerTests : ScaleoutHubLifetimeManagerTests<MongoDbScaleoutBackplane>
{
    public override MongoDbScaleoutBackplane CreateBackplane()
    {
        return new MongoDbScaleoutBackplane();
    }

    public override HubLifetimeManager<Hub> CreateNewHubLifetimeManager()
    {
        return CreateNewHubLifetimeManager(CreateBackplane());
    }

    public override HubLifetimeManager<Hub> CreateNewHubLifetimeManager(MongoDbScaleoutBackplane backplane)
    {
        return new MongoDbHubLifetimeManager<Hub>(
            NullLogger<MongoDbHubLifetimeManager<Hub>>.Instance,
            Options.Create(new MongoDbSignalROptions { AckTimeout = TimeSpan.FromSeconds(5) }),
            new TestHubProtocolResolver(new JsonHubProtocol()),
            backplane.Backplane);
    }
}
