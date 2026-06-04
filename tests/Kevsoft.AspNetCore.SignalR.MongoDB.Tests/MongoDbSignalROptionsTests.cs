using Kevsoft.AspNetCore.SignalR.MongoDB;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoDbSignalROptionsTests
{
    [Fact]
    public void DefaultsMatchBaselineContract()
    {
        var options = new MongoDbSignalROptions();

        Assert.Null(options.ConnectionString);
        Assert.Null(options.DatabaseName);
        Assert.Equal(MongoDbSignalROptions.DefaultCollectionName, options.CollectionName);
        Assert.Equal(MongoDbSignalRTransportMode.ChangeStreams, options.TransportMode);
        Assert.Null(options.MongoClientFactory);
        Assert.Null(options.ConfigureClientSettings);
        Assert.Null(options.ChannelPrefix);
        Assert.Equal(TimeSpan.FromSeconds(30), options.AckTimeout);
        Assert.Equal(TimeSpan.FromSeconds(1), options.TailableAwaitMaxAwaitTime);
        Assert.Equal(MongoDbSignalROptions.DefaultTailableCollectionSizeBytes, options.TailableCollectionSizeBytes);
        Assert.Equal(TimeSpan.FromDays(1), options.MessageTtl);
        Assert.Null(options.CheckpointStore);
        Assert.True(options.CreateCollectionIfMissing);
        Assert.True(options.CreateIndexes);
        Assert.True(options.RunCollectionSetupOnStartup);
    }
}
