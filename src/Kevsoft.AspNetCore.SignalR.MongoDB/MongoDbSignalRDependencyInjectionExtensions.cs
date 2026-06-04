using Kevsoft.AspNetCore.SignalR.MongoDB;
using Kevsoft.AspNetCore.SignalR.MongoDB.Internal;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for configuring MongoDB-based scale-out for SignalR.
/// </summary>
public static class MongoDbSignalRDependencyInjectionExtensions
{
    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(this ISignalRServerBuilder signalrBuilder)
    {
        return AddMongoDb(signalrBuilder, static _ => { });
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(this ISignalRServerBuilder signalrBuilder, string connectionString)
    {
        return AddMongoDb(signalrBuilder, connectionString, static _ => { });
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="configure">A callback used to configure MongoDB scale-out options.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(
        this ISignalRServerBuilder signalrBuilder,
        Action<MongoDbSignalROptions> configure)
    {
        ArgumentNullException.ThrowIfNull(signalrBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        signalrBuilder.Services.Configure(configure);
        AddMongoDbServices(signalrBuilder.Services);

        return signalrBuilder;
    }

    /// <summary>
    /// Adds MongoDB scale-out to a SignalR server.
    /// </summary>
    /// <param name="signalrBuilder">The SignalR server builder.</param>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <param name="configure">A callback used to configure MongoDB scale-out options.</param>
    /// <returns>The same <see cref="ISignalRServerBuilder"/> instance for chaining.</returns>
    public static ISignalRServerBuilder AddMongoDb(
        this ISignalRServerBuilder signalrBuilder,
        string connectionString,
        Action<MongoDbSignalROptions> configure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentNullException.ThrowIfNull(configure);

        return AddMongoDb(signalrBuilder, options =>
        {
            ApplyConnectionString(options, connectionString);
            configure(options);
        });
    }

    private static void AddMongoDbServices(IServiceCollection services)
    {
        services.TryAddSingleton<IMessageCheckpointStore, InMemoryMessageCheckpointStore>();
        services.TryAddSingleton<IBackplaneEnvelopeSerializer, BsonBackplaneEnvelopeSerializer>();
        services.TryAddSingleton<IMongoDbSignalRClientFactory, DefaultMongoDbSignalRClientFactory>();
        services.TryAddSingleton(static serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<MongoDbSignalROptions>>().Value;
            var clientFactory = serviceProvider.GetRequiredService<IMongoDbSignalRClientFactory>();
            return clientFactory.CreateClient().GetDatabase(options.DatabaseName);
        });

        services.AddTransient(MongoDbSignalRServiceFactory.CreateBackplane);
        services.AddTransient(static serviceProvider =>
            (IMongoDbSignalRCollectionInitializer)MongoDbSignalRServiceFactory.CreateBackplane(serviceProvider));
        services.AddSingleton(typeof(HubLifetimeManager<>), typeof(MongoDbHubLifetimeManager<>));
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<MongoDbSignalROptions>, MongoDbSignalROptionsValidator>());

        var optionsBuilder = services.AddOptions<MongoDbSignalROptions>();
        optionsBuilder.Configure<IMessageCheckpointStore>(static (options, checkpointStore) =>
        {
            options.CheckpointStore ??= checkpointStore;
        });
        optionsBuilder.ValidateOnStart();
    }

    private static void ApplyConnectionString(MongoDbSignalROptions options, string connectionString)
    {
        options.ConnectionString = connectionString;

        if (!string.IsNullOrWhiteSpace(options.DatabaseName))
        {
            return;
        }

        var mongoUrl = MongoUrl.Create(connectionString);
        if (!string.IsNullOrWhiteSpace(mongoUrl.DatabaseName))
        {
            options.DatabaseName = mongoUrl.DatabaseName;
        }
    }
}
