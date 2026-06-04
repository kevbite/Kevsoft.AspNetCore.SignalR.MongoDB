using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public class MongoDbDependencyInjectionTests
{
    [Fact]
    public async Task AddMongoDbRegistersMongoDbHubLifetimeManager()
    {
        await using var provider = CreateServices(options =>
        {
            options.ConnectionString = "mongodb://localhost:27017";
            options.DatabaseName = "app";
        }).BuildServiceProvider();

        var lifetimeManager = provider.GetRequiredService<HubLifetimeManager<Hub>>();

        Assert.IsType<MongoDbHubLifetimeManager<Hub>>(lifetimeManager);
    }

    [Fact]
    public void ConnectionStringOverloadMapsOptionsAndDatabaseName()
    {
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb("mongodb://localhost:27017/app")
            .Services
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;

        Assert.Equal("mongodb://localhost:27017/app", options.ConnectionString);
        Assert.Equal("app", options.DatabaseName);
    }

    [Fact]
    public void ConnectionStringAndConfigureOverloadAppliesCallback()
    {
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb("mongodb://localhost:27017/app", options =>
            {
                options.CollectionName = "custom_messages";
                options.TransportMode = MongoDbSignalRTransportMode.TailableAwait;
            })
            .Services
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;

        Assert.Equal("custom_messages", options.CollectionName);
        Assert.Equal(MongoDbSignalRTransportMode.TailableAwait, options.TransportMode);
    }

    [Fact]
    public void ConfigureOverloadUsesCustomMongoClientFactory()
    {
        var factoryCalled = false;
        using var provider = CreateServices(options =>
        {
            options.DatabaseName = "app";
            options.MongoClientFactory = _ =>
            {
                factoryCalled = true;
                return new MongoClient("mongodb://localhost:27017");
            };
        }).BuildServiceProvider();

        _ = provider.GetRequiredService<IMongoDatabase>();

        Assert.True(factoryCalled);
    }

    [Theory]
    [InlineData(MongoDbSignalRTransportMode.ChangeStreams, typeof(MongoDbChangeStreamBackplane))]
    [InlineData(MongoDbSignalRTransportMode.TailableAwait, typeof(MongoDbTailableAwaitBackplane))]
    public async Task TransportModeSelectsBackplane(MongoDbSignalRTransportMode transportMode, Type expectedBackplaneType)
    {
        await using var provider = CreateServices(options =>
        {
            options.ConnectionString = "mongodb://localhost:27017";
            options.DatabaseName = "app";
            options.TransportMode = transportMode;
        }).BuildServiceProvider();

        var backplane = provider.GetRequiredService<IMongoSignalRBackplane>();

        Assert.IsType(expectedBackplaneType, backplane);
    }

    [Fact]
    public void CustomCheckpointStoreIsAppliedToOptions()
    {
        var services = new ServiceCollection()
            .AddLogging();
        services.AddSingleton<IMessageCheckpointStore, TestCheckpointStore>();
        services
            .AddSignalR()
            .AddMongoDb(options =>
            {
                options.ConnectionString = "mongodb://localhost:27017";
                options.DatabaseName = "app";
            });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;

        Assert.IsType<TestCheckpointStore>(options.CheckpointStore);
    }

    [Fact]
    public void InvalidOptionsFailValidation()
    {
        using var provider = CreateServices(options =>
        {
            options.ConnectionString = "mongodb://localhost:27017";
            options.DatabaseName = "";
            options.AckTimeout = TimeSpan.Zero;
        }).BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value);

        Assert.Contains("DatabaseName must be configured.", exception.Failures);
        Assert.Contains("AckTimeout must be greater than zero.", exception.Failures);
    }

    [Fact]
    public async Task ChannelPrefixIsComposedWithHubType()
    {
        var backplane = new FakeMongoSignalRBackplane();
        using var manager = new MongoDbHubLifetimeManager<Hub>(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<MongoDbHubLifetimeManager<Hub>>.Instance,
            Options.Create(new MongoDbSignalROptions
            {
                ChannelPrefix = "app",
                AckTimeout = TimeSpan.FromSeconds(1)
            }),
            new TestHubProtocolResolver(new Microsoft.AspNetCore.SignalR.Protocol.JsonHubProtocol()),
            backplane);

        await manager.SendAllAsync("Hello", ["World"]);

        Assert.StartsWith("app:Microsoft.AspNetCore.SignalR.Hub:", Assert.Single(backplane.Published).Channel);
    }

    private static IServiceCollection CreateServices(Action<MongoDbSignalROptions> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddMongoDb(configure);
        return services;
    }

    private sealed class TestCheckpointStore : IMessageCheckpointStore
    {
        public ValueTask<MongoDbBackplaneCheckpoint?> GetAsync(
            string streamId,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult<MongoDbBackplaneCheckpoint?>(null);
        }

        public ValueTask SetAsync(
            string streamId,
            MongoDbBackplaneCheckpoint checkpoint,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask ClearAsync(string streamId, CancellationToken cancellationToken = default)
        {
            return ValueTask.CompletedTask;
        }
    }
}
