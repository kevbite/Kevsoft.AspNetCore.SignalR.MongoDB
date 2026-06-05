using Kevsoft.AspNetCore.SignalR.MongoDB;
using MongoDB.Driver;

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
        Assert.Null(options.MongoDatabaseFactory);
        Assert.Null(options.ConfigureClientSettings);
        Assert.Null(options.ChannelPrefix);
        Assert.Equal(TimeSpan.FromSeconds(30), options.AckTimeout);
        Assert.Equal(TimeSpan.FromSeconds(1), options.TailableAwaitMaxAwaitTime);
        Assert.Equal(MongoDbSignalROptions.DefaultTailableCollectionSizeBytes, options.TailableCollectionSizeBytes);
        Assert.Equal(TimeSpan.FromDays(1), options.MessageTtl);
        Assert.Equal(TimeSpan.FromMinutes(2), options.ConnectionPresenceTtl);
        Assert.Null(options.CheckpointStore);
        Assert.True(options.CreateCollectionIfMissing);
        Assert.True(options.CreateIndexes);
        Assert.True(options.RunCollectionSetupOnStartup);
    }

    [Fact]
    public void UseConnectionStringSetsConnectionString()
    {
        var options = new MongoDbSignalROptions();

        options.UseConnectionString("mongodb://localhost:27017");

        Assert.Equal("mongodb://localhost:27017", options.ConnectionString);
    }

    [Fact]
    public void UseConnectionStringInfersDatabaseNameFromUrl()
    {
        var options = new MongoDbSignalROptions();

        options.UseConnectionString("mongodb://localhost:27017/mydb");

        Assert.Equal("mongodb://localhost:27017/mydb", options.ConnectionString);
        Assert.Equal("mydb", options.DatabaseName);
    }

    [Fact]
    public void UseConnectionStringDoesNotOverwriteExistingDatabaseName()
    {
        var options = new MongoDbSignalROptions();
        options.UseConnectionString("mongodb://localhost:27017/existing");

        options.UseConnectionString("mongodb://localhost:27017/fromurl");

        Assert.Equal("existing", options.DatabaseName);
    }

    [Fact]
    public void UseConnectionStringWithDatabaseNameOverloadSetsBoth()
    {
        var options = new MongoDbSignalROptions();

        options.UseConnectionString("mongodb://localhost:27017", "explicit_db");

        Assert.Equal("mongodb://localhost:27017", options.ConnectionString);
        Assert.Equal("explicit_db", options.DatabaseName);
    }

    [Fact]
    public void UseConnectionStringWithDatabaseNameOverloadOverwritesExistingDatabaseName()
    {
        var options = new MongoDbSignalROptions();
        options.UseConnectionString("mongodb://localhost:27017/old");

        options.UseConnectionString("mongodb://localhost:27017", "new_db");

        Assert.Equal("new_db", options.DatabaseName);
    }

    [Fact]
    public void UseMongoClientSetsBothFactoryAndDatabaseName()
    {
        var options = new MongoDbSignalROptions();
        IMongoClient? capturedSp = null;
        Func<IServiceProvider, IMongoClient> factory = _ =>
        {
            capturedSp = new MongoClient("mongodb://localhost:27017");
            return capturedSp;
        };

        options.UseMongoClient(factory, "client_db");

        Assert.Same(factory, options.MongoClientFactory);
        Assert.Equal("client_db", options.DatabaseName);
    }

    [Fact]
    public void UseMongoDatabaseSetsMongoDatabaseFactory()
    {
        var options = new MongoDbSignalROptions();
        var fakeDb = new MongoClient("mongodb://localhost:27017").GetDatabase("app");
        Func<IServiceProvider, IMongoDatabase> factory = _ => fakeDb;

        options.UseMongoDatabase(factory);

        Assert.Same(factory, options.MongoDatabaseFactory);
    }

    [Fact]
    public void UseChangeStreamsSetsTransportModeAndDefaultMessageTtl()
    {
        var options = new MongoDbSignalROptions();

        options.UseChangeStreams();

        Assert.Equal(MongoDbSignalRTransportMode.ChangeStreams, options.TransportMode);
        Assert.Equal(TimeSpan.FromDays(1), options.MessageTtl);
    }

    [Fact]
    public void UseChangeStreamsAppliesConfigureCallback()
    {
        var options = new MongoDbSignalROptions();

        options.UseChangeStreams(o => o.MessageTtl = TimeSpan.FromHours(6));

        Assert.Equal(MongoDbSignalRTransportMode.ChangeStreams, options.TransportMode);
        Assert.Equal(TimeSpan.FromHours(6), options.MessageTtl);
    }

    [Fact]
    public void UseTailableAwaitSetsTransportModeAndDefaults()
    {
        var options = new MongoDbSignalROptions();

        options.UseTailableAwait();

        Assert.Equal(MongoDbSignalRTransportMode.TailableAwait, options.TransportMode);
        Assert.Equal(TimeSpan.FromSeconds(1), options.TailableAwaitMaxAwaitTime);
        Assert.Equal(MongoDbSignalROptions.DefaultTailableCollectionSizeBytes, options.TailableCollectionSizeBytes);
    }

    [Fact]
    public void UseTailableAwaitAppliesConfigureCallback()
    {
        var options = new MongoDbSignalROptions();

        options.UseTailableAwait(o =>
        {
            o.MaxAwaitTime = TimeSpan.FromMilliseconds(500);
            o.CollectionSizeBytes = 1024 * 1024;
        });

        Assert.Equal(MongoDbSignalRTransportMode.TailableAwait, options.TransportMode);
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.TailableAwaitMaxAwaitTime);
        Assert.Equal(1024 * 1024, options.TailableCollectionSizeBytes);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void UseConnectionStringThrowsOnNullOrWhitespace(string? value)
    {
        var options = new MongoDbSignalROptions();

        Assert.ThrowsAny<ArgumentException>(() => options.UseConnectionString(value!));
    }

    [Fact]
    public void UseMongoClientThrowsOnNullFactory()
    {
        var options = new MongoDbSignalROptions();

        Assert.Throws<ArgumentNullException>(() => options.UseMongoClient(null!, "db"));
    }

    [Fact]
    public void UseMongoDatabaseThrowsOnNullFactory()
    {
        var options = new MongoDbSignalROptions();

        Assert.Throws<ArgumentNullException>(() => options.UseMongoDatabase(null!));
    }
}
