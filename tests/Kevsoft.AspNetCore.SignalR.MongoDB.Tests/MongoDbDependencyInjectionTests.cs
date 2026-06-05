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
            options.UseConnectionString("mongodb://localhost:27017", "app");
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
                options.UseTailableAwait();
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
            options.UseMongoClient(_ =>
            {
                factoryCalled = true;
                return new MongoClient("mongodb://localhost:27017");
            }, "app");
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
            options.UseConnectionString("mongodb://localhost:27017", "app");
            if (transportMode == MongoDbSignalRTransportMode.TailableAwait)
                options.UseTailableAwait();
            else
                options.UseChangeStreams();
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
                options.UseConnectionString("mongodb://localhost:27017", "app");
            });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;

        Assert.IsType<TestCheckpointStore>(options.CheckpointStore);
    }

    [Fact]
    public void InvalidOptionsFailValidation()
    {
        // UseConnectionString("mongodb://localhost:27017") infers no DatabaseName (no DB in URL),
        // so the validator fires for both missing database and zero AckTimeout.
        using var provider = CreateServices(options =>
        {
            options.UseConnectionString("mongodb://localhost:27017");
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

    [Fact]
    public void ServiceProviderConfigureCallbackAppliesOptions()
    {
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb((_, options) =>
            {
                options.UseConnectionString("mongodb://localhost:27017", "app_sp");
            })
            .Services
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;

        Assert.Equal("mongodb://localhost:27017", options.ConnectionString);
        Assert.Equal("app_sp", options.DatabaseName);
    }

    [Fact]
    public void ServiceProviderConfigureCallbackReceivesServiceProvider()
    {
        IServiceProvider? capturedSp = null;

        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb((sp, options) =>
            {
                capturedSp = sp;
                options.UseConnectionString("mongodb://localhost:27017", "app");
            })
            .Services
            .BuildServiceProvider();

        _ = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;

        Assert.NotNull(capturedSp);
    }

    [Fact]
    public void ConnectionStringAndServiceProviderConfigureAppliesCallback()
    {
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb("mongodb://localhost:27017/app", (_, options) =>
            {
                options.CollectionName = "sp_messages";
            })
            .Services
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;

        Assert.Equal("app", options.DatabaseName);
        Assert.Equal("sp_messages", options.CollectionName);
    }

    [Fact]
    public void MongoDatabaseFactoryOverloadUsesFactory()
    {
        var fakeDatabase = new MongoClient("mongodb://localhost:27017").GetDatabase("app");
        var factoryCalled = false;

        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb(options => options.UseMongoDatabase(_ =>
            {
                factoryCalled = true;
                return fakeDatabase;
            }))
            .Services
            .BuildServiceProvider();

        var resolvedDb = provider.GetRequiredService<IMongoDatabase>();

        Assert.True(factoryCalled);
        Assert.Same(fakeDatabase, resolvedDb);
    }

    [Fact]
    public void MongoDatabaseFactorySkipsConnectionStringValidation()
    {
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb(options =>
                options.UseMongoDatabase(_ => new MongoClient("mongodb://localhost:27017").GetDatabase("app")))
            .Services
            .BuildServiceProvider();

        var exception = Record.Exception(
            () => provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value);

        Assert.Null(exception);
    }

    [Fact]
    public void MongoDatabaseFactorySkipsDatabaseNameValidation()
    {
        // UseMongoDatabase bypasses DatabaseName validation; leaving DatabaseName null is fine.
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb(options =>
                options.UseMongoDatabase(_ => new MongoClient("mongodb://localhost:27017").GetDatabase("app")))
            .Services
            .BuildServiceProvider();

        var exception = Record.Exception(
            () => provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value);

        Assert.Null(exception);
    }

    [Fact]
    public void MongoDatabaseFactoryWithConfigureOverloadAppliesBoth()
    {
        var fakeDatabase = new MongoClient("mongodb://localhost:27017").GetDatabase("app");

        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb(options =>
            {
                options.UseMongoDatabase(_ => fakeDatabase);
                options.CollectionName = "custom_col";
            })
            .Services
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;
        var resolvedDb = provider.GetRequiredService<IMongoDatabase>();

        Assert.Equal("custom_col", options.CollectionName);
        Assert.Same(fakeDatabase, resolvedDb);
    }

    [Fact]
    public void MongoDatabaseFactoryWithServiceProviderConfigureAppliesBoth()
    {
        var fakeDatabase = new MongoClient("mongodb://localhost:27017").GetDatabase("app");
        IServiceProvider? capturedSp = null;

        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb((sp, options) =>
            {
                capturedSp = sp;
                options.UseMongoDatabase(_ => fakeDatabase);
                options.CollectionName = "sp_col";
            })
            .Services
            .BuildServiceProvider();

        var options = provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;
        var resolvedDb = provider.GetRequiredService<IMongoDatabase>();

        Assert.Equal("sp_col", options.CollectionName);
        Assert.Same(fakeDatabase, resolvedDb);
        Assert.NotNull(capturedSp);
    }

    [Fact]
    public void InvalidOptionsWithoutMongoDatabaseFactoryStillFailValidation()
    {
        // UseConnectionString("mongodb://localhost:27017") has no DB in URL → DatabaseName stays null.
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb(options => options.UseConnectionString("mongodb://localhost:27017"))
            .Services
            .BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value);

        Assert.Contains("DatabaseName must be configured.", exception.Failures);
    }

    [Fact]
    public void MissingConnectionStringAndFactoryFailsValidation()
    {
        using var provider = new ServiceCollection()
            .AddLogging()
            .AddSignalR()
            .AddMongoDb(static _ => { })
            .Services
            .BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value);

        Assert.Contains(
            "Either ConnectionString, MongoClientFactory, or MongoDatabaseFactory must be configured.",
            exception.Failures);
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
